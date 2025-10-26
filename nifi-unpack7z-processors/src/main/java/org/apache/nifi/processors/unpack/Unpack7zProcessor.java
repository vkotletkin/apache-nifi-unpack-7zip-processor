/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.unpack;

import org.apache.commons.compress.archivers.sevenz.SevenZArchiveEntry;
import org.apache.commons.compress.archivers.sevenz.SevenZFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"7zip", "archive", "extract", "unpack"})
@CapabilityDescription("Extracts files from 7zip archive and creates individual flowfiles for each extracted file. " +
        "WARNING: This processor creates temporary files on disk which may consume significant disk space. " +
        "Ensure adequate temporary storage is available and monitor disk usage.")
@ReadsAttributes({@ReadsAttribute(attribute = "filename", description = "The name of the archive file")})
@WritesAttributes({
        @WritesAttribute(attribute = "filename", description = "The name of the extracted file"),
        @WritesAttribute(attribute = "path", description = "The path of the extracted file within the archive"),
        @WritesAttribute(attribute = "original.archive.name", description = "The name of the original archive")
})
public class Unpack7zProcessor extends AbstractProcessor {


    // === КОНФИГУРАЦИЯ ПРОЦЕССОРА ===

    /**
     * Свойство: Удалить исходный архив после распаковки
     * - По умолчанию false для сохранения исходных данных
     * - Установка true может быть полезна для экономии места
     */
    public static final PropertyDescriptor DELETE_ORIGINAL = new PropertyDescriptor.Builder()
            .name("Delete Original Archive")
            .description("Whether to delete the original archive after successful extraction")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    /**
     * Свойство: Максимальный размер извлекаемого файла
     * - Защита от извлечения слишком больших файлов
     * - 0 = без ограничений (может быть опасно)
     */
    public static final PropertyDescriptor MAX_FILE_SIZE = new PropertyDescriptor.Builder()
            .name("Max File Size")
            .description("Maximum size in bytes for individual files to extract (0 for unlimited)")
            .required(true)
            .defaultValue("0")
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .build();

    /**
     * Свойство: Размер буфера для чтения
     * - Оптимизация производительности ввода-вывода
     * - Большие буферы могут улучшить производительность, но потребляют больше памяти
     */
    public static final PropertyDescriptor BUFFER_SIZE = new PropertyDescriptor.Builder()
            .name("Buffer Size")
            .description("Buffer size in bytes for reading archive entries (default: 65536B)")
            .required(true)
            .defaultValue("65536B")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Extracted files are routed to this relationship")
            .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("Original archive file")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Files that could not be processed are routed to this relationship")
            .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        this.descriptors = List.of(DELETE_ORIGINAL, MAX_FILE_SIZE, BUFFER_SIZE);
        this.relationships = Set.of(REL_SUCCESS, REL_ORIGINAL, REL_FAILURE);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return this.descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        // Initialization code if needed
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        // === ШАГ 1: ПОЛУЧЕНИЕ ВХОДНОГО FLOWFILE ===

        FlowFile flowFile = session.get();

        if (flowFile == null) {
            return;
        }

        // === ШАГ 2: ЧТЕНИЕ КОНФИГУРАЦИИ ===
        final boolean deleteOriginal = context.getProperty(DELETE_ORIGINAL).asBoolean();
        final long maxFileSize = context.getProperty(MAX_FILE_SIZE).asLong();
        final int bufferSize = context.getProperty(BUFFER_SIZE).asDataSize(DataUnit.B).intValue();

        // === ШАГ 3: ПОДГОТОВКА ПЕРЕМЕННЫХ ДЛЯ ОБРАБОТКИ ===
        final AtomicReference<String> errorMessage = new AtomicReference<>();
        final List<FlowFile> extractedFiles = new ArrayList<>();

        try {
            // === ШАГ 4: ЧТЕНИЕ И ОБРАБОТКА АРХИВА ===
            session.read(flowFile, rawInput -> {
                Path tempFile = null;
                try {
                    // === ШАГ 4.1: СОЗДАНИЕ ВРЕМЕННОГО ФАЙЛА ===
                    // ВНИМАНИЕ: Это создает уязвимость - временные файлы могут заполнить диск
                    // SevenZFile требует File, а не InputStream, поэтому вынуждены создавать временный файл
                    tempFile = Files.createTempFile("nifi_7zip_", ".7z");

                    // Копируем содержимое FlowFile во временный файл
                    // ВНИМАНИЕ: Большие архивы могут исчерпать место на диске
                    Files.copy(rawInput, tempFile, StandardCopyOption.REPLACE_EXISTING);

                    // === ШАГ 4.2: ОТКРЫТИЕ 7Z АРХИВА ===
                    // Используем Builder Pattern
                    try (SevenZFile sevenZFile = SevenZFile.builder().setFile(tempFile.toFile()).get()) {

                        SevenZArchiveEntry entry;

                        // === ШАГ 4.3: ИТЕРАЦИЯ ПО ЭЛЕМЕНТАМ АРХИВА ===
                        while ((entry = sevenZFile.getNextEntry()) != null) {
                            final SevenZArchiveEntry currentEntry = entry; // Создаем effectively final переменную

                            if (!currentEntry.isDirectory()) {
                                // === ШАГ 4.4: ПРОВЕРКА РАЗМЕРА ФАЙЛА ===
                                final long entrySize = currentEntry.getSize();
                                if (maxFileSize > 0 && entrySize > maxFileSize) {
                                    getLogger().warn("Skipping file {} due to size limit ({} > {})",
                                            currentEntry.getName(), entrySize, maxFileSize);
                                    continue;
                                }

                                // === ШАГ 4.5: СОЗДАНИЕ НОВОГО FLOWFILE ===
                                FlowFile extractedFlowFile = session.create(flowFile);

                                // === ШАГ 4.6: ЗАПИСЬ ДАННЫХ В FLOWFILE ===
                                extractedFlowFile = session.write(extractedFlowFile, outputStream -> {
                                    try {
                                        // === ШАГ 4.6.1: ОБРАБОТКА ФАЙЛОВ ИЗВЕСТНОГО РАЗМЕРА ===
                                        if (entrySize > 0) {
                                            byte[] buffer = new byte[bufferSize];
                                            long remaining = entrySize;

                                            // Чтение данных блоками для оптимизации памяти
                                            while (remaining > 0) {
                                                int bytesToRead = (int) Math.min(buffer.length, remaining);
                                                int bytesRead = sevenZFile.read(buffer, 0, bytesToRead);
                                                if (bytesRead == -1) {
                                                    break;
                                                }
                                                outputStream.write(buffer, 0, bytesRead);
                                                remaining -= bytesRead;
                                            }
                                        } else {
                                            // === ШАГ 4.6.2: ОБРАБОТКА ФАЙЛОВ НЕИЗВЕСТНОГО РАЗМЕРА ===
                                            // ВНИМАНИЕ: Это может привести к OOM если файл очень большой
                                            byte[] buffer = new byte[bufferSize];
                                            int bytesRead;
                                            while ((bytesRead = sevenZFile.read(buffer)) != -1) {
                                                outputStream.write(buffer, 0, bytesRead);
                                            }
                                        }
                                    } catch (IOException e) {
                                        throw new UncheckedIOException("Failed to extract file: " + currentEntry.getName(), e);
                                    }
                                });

                                // === ШАГ 4.7: УСТАНОВКА АТРИБУТОВ ===
                                Map<String, String> attributes = new HashMap<>();
                                String entryName = currentEntry.getName();

                                attributes.put("filename", getFileName(entryName));
                                attributes.put("path", getDirectoryPath(entryName));
                                attributes.put("original.archive.name", flowFile.getAttribute("filename"));
                                attributes.put("file.size", String.valueOf(entrySize > 0 ? entrySize : extractedFlowFile.getSize()));

                                // Временные метки
                                if (currentEntry.getLastModifiedDate() != null) {
                                    attributes.put("file.lastModified",
                                            String.valueOf(currentEntry.getLastModifiedDate().getTime()));
                                } else {
                                    attributes.put("file.lastModified",
                                            String.valueOf(System.currentTimeMillis()));
                                }

                                extractedFlowFile = session.putAllAttributes(extractedFlowFile, attributes);
                                extractedFiles.add(extractedFlowFile);

                                getLogger().info("Extracted file: {} ({} bytes) from archive",
                                        currentEntry.getName(), extractedFlowFile.getSize());
                            }
                        }
                    }
                } catch (Exception e) {
                    // === ШАГ 4.8: ОБРАБОТКА ОШИБОК ===
                    errorMessage.set("Failed to extract 7zip archive: " + e.getMessage());
                    getLogger().error("Error extracting 7zip archive", e);
                } finally {
                    // КРИТИЧЕСКИ ВАЖНО: Удаляем временный файл, чтобы избежать утечки дискового пространства
                    if (tempFile != null) {
                        try {
                            Files.deleteIfExists(tempFile);
                        } catch (IOException e) {
                            getLogger().warn("Could not delete temporary file: {}", tempFile, e);
                        }
                    }
                }
            });

            // === ШАГ 5: ОБРАБОТКА РЕЗУЛЬТАТОВ ===

            // === ШАГ 5.1: ПЕРЕДАЧА ИЗВЛЕЧЕННЫХ ФАЙЛОВ ===
            if (!extractedFiles.isEmpty()) {
                session.transfer(extractedFiles, REL_SUCCESS);
                getLogger().info("Transferred {} extracted files to success", extractedFiles.size());
            }

            // === ШАГ 5.2: ОБРАБОТКА ИСХОДНОГО АРХИВА ===
            if (errorMessage.get() == null) {
                if (deleteOriginal) {
                    session.remove(flowFile);
                    getLogger().info("Successfully extracted {} files and removed original archive", extractedFiles.size());
                } else {
                    session.transfer(flowFile, REL_ORIGINAL);
                    getLogger().info("Successfully extracted {} files, original archive preserved",
                            extractedFiles.size());
                }
            } else {
                // === ШАГ 5.3: ОБРАБОТКА ОШИБОК ===
                // Удаляем частично созданные FlowFile, чтобы избежать утечки
                for (FlowFile extractedFile : extractedFiles) {
                    session.remove(extractedFile);
                }
                session.transfer(flowFile, REL_FAILURE);
                getLogger().error("Failed to process archive: {}", errorMessage.get());
            }

        } catch (ProcessException e) {
            // === ШАГ 6: ОБРАБОТКА КРИТИЧЕСКИХ ОШИБОК ===
            getLogger().error("Failed to process flowfile", e);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    /**
     * Извлекает имя файла из полного пути
     */
    private String getFileName(String fullPath) {
        if (fullPath == null) return "unknown";
        int lastSeparator = Math.max(
                fullPath.lastIndexOf('/'),
                fullPath.lastIndexOf('\\')
        );
        return lastSeparator >= 0 ? fullPath.substring(lastSeparator + 1) : fullPath;
    }

    /**
     * Извлекает путь директории из полного пути
     */
    private String getDirectoryPath(String fullPath) {
        if (fullPath == null) return "";
        int lastSeparator = Math.max(
                fullPath.lastIndexOf('/'),
                fullPath.lastIndexOf('\\')
        );
        return lastSeparator >= 0 ? fullPath.substring(0, lastSeparator) : "";
    }
}