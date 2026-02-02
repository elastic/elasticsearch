/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;

import java.lang.reflect.Constructor;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Registry for FormatReader implementations, keyed by format name and file extension.
 * Allows pluggable discovery of format readers based on explicit format specification
 * or file extension inference.
 */
public class FormatReaderRegistry {
    private final Map<String, Supplier<FormatReader>> byName = new ConcurrentHashMap<>();
    private final Map<String, Supplier<FormatReader>> byExtension = new ConcurrentHashMap<>();

    /**
     * Registers a FormatReader prototype. The registry will use reflection to create
     * new instances when needed.
     *
     * @param prototype a prototype instance used to extract metadata (format name, extensions)
     * @throws IllegalArgumentException if prototype is null or has invalid metadata
     */
    public void register(FormatReader prototype) {
        if (prototype == null) {
            throw new IllegalArgumentException("Prototype cannot be null");
        }

        String formatName = prototype.formatName();
        if (formatName == null || formatName.isEmpty()) {
            throw new IllegalArgumentException("Format name cannot be null or empty");
        }

        Supplier<FormatReader> supplier = () -> createInstance(prototype.getClass());
        byName.put(formatName.toLowerCase(Locale.ROOT), supplier);

        for (String ext : prototype.fileExtensions()) {
            if (ext != null && !ext.isEmpty()) {
                String normalizedExt = ext.toLowerCase(Locale.ROOT);
                // Ensure extension starts with a dot
                if (!normalizedExt.startsWith(".")) {
                    normalizedExt = "." + normalizedExt;
                }
                byExtension.put(normalizedExt, supplier);
            }
        }
    }

    /**
     * Registers a FormatReader with a custom supplier.
     *
     * @param formatName the format name
     * @param fileExtensions the file extensions this reader handles
     * @param supplier the supplier to create FormatReader instances
     */
    public void register(String formatName, java.util.List<String> fileExtensions, Supplier<FormatReader> supplier) {
        if (formatName == null || formatName.isEmpty()) {
            throw new IllegalArgumentException("Format name cannot be null or empty");
        }
        if (supplier == null) {
            throw new IllegalArgumentException("Supplier cannot be null");
        }

        byName.put(formatName.toLowerCase(Locale.ROOT), supplier);

        if (fileExtensions != null) {
            for (String ext : fileExtensions) {
                if (ext != null && !ext.isEmpty()) {
                    String normalizedExt = ext.toLowerCase(Locale.ROOT);
                    // Ensure extension starts with a dot
                    if (!normalizedExt.startsWith(".")) {
                        normalizedExt = "." + normalizedExt;
                    }
                    byExtension.put(normalizedExt, supplier);
                }
            }
        }
    }

    /**
     * Unregisters a format reader by name.
     *
     * @param formatName the format name to unregister
     * @return the previously registered supplier, or null if none was registered
     */
    public Supplier<FormatReader> unregister(String formatName) {
        if (formatName == null || formatName.isEmpty()) {
            return null;
        }
        return byName.remove(formatName.toLowerCase(Locale.ROOT));
    }

    /**
     * Gets a FormatReader by explicit format name.
     *
     * @param formatName the format name (e.g., "parquet", "csv")
     * @return a new FormatReader instance
     * @throws IllegalArgumentException if no reader is registered for the format
     */
    public FormatReader getByName(String formatName) {
        if (formatName == null || formatName.isEmpty()) {
            throw new IllegalArgumentException("Format name cannot be null or empty");
        }

        Supplier<FormatReader> supplier = byName.get(formatName.toLowerCase(Locale.ROOT));
        if (supplier == null) {
            throw new IllegalArgumentException("No format reader registered for format: " + formatName);
        }
        return supplier.get();
    }

    /**
     * Gets a FormatReader by inferring from object name (file extension).
     *
     * @param objectName the object name (e.g., "data.parquet", "sales.csv")
     * @return a new FormatReader instance
     * @throws IllegalArgumentException if no reader can be inferred from the extension
     */
    public FormatReader getByExtension(String objectName) {
        if (objectName == null || objectName.isEmpty()) {
            throw new IllegalArgumentException("Object name cannot be null or empty");
        }

        // Find the last dot in the object name
        int lastDot = objectName.lastIndexOf('.');
        if (lastDot < 0 || lastDot == objectName.length() - 1) {
            throw new IllegalArgumentException("Cannot infer format from object name without extension: " + objectName);
        }

        String extension = objectName.substring(lastDot).toLowerCase(Locale.ROOT);
        Supplier<FormatReader> supplier = byExtension.get(extension);
        if (supplier == null) {
            throw new IllegalArgumentException("No format reader registered for extension: " + extension);
        }
        return supplier.get();
    }

    /**
     * Checks if a reader is registered for the given format name.
     *
     * @param formatName the format name to check
     * @return true if a reader is registered, false otherwise
     */
    public boolean hasFormat(String formatName) {
        if (formatName == null || formatName.isEmpty()) {
            return false;
        }
        return byName.containsKey(formatName.toLowerCase(Locale.ROOT));
    }

    /**
     * Checks if a reader is registered for the given file extension.
     *
     * @param extension the file extension to check (with or without leading dot)
     * @return true if a reader is registered, false otherwise
     */
    public boolean hasExtension(String extension) {
        if (extension == null || extension.isEmpty()) {
            return false;
        }
        String normalizedExt = extension.toLowerCase(Locale.ROOT);
        if (!normalizedExt.startsWith(".")) {
            normalizedExt = "." + normalizedExt;
        }
        return byExtension.containsKey(normalizedExt);
    }

    /**
     * Creates a new instance of a FormatReader using reflection.
     * Attempts to use a no-arg constructor.
     *
     * @param clazz the FormatReader class
     * @return a new instance
     * @throws RuntimeException if instantiation fails
     */
    private FormatReader createInstance(Class<? extends FormatReader> clazz) {
        try {
            Constructor<? extends FormatReader> constructor = clazz.getDeclaredConstructor();
            constructor.setAccessible(true);
            return constructor.newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create FormatReader instance for class: " + clazz.getName(), e);
        }
    }
}
