/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;

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

    public void register(FormatReader reader) {
        if (reader == null) {
            throw new IllegalArgumentException("Reader cannot be null");
        }

        String formatName = reader.formatName();
        if (formatName == null || formatName.isEmpty()) {
            throw new IllegalArgumentException("Format name cannot be null or empty");
        }

        // Store the reader instance directly - FormatReaders are expected to be thread-safe
        Supplier<FormatReader> supplier = () -> reader;
        byName.put(formatName.toLowerCase(Locale.ROOT), supplier);

        for (String ext : reader.fileExtensions()) {
            if (ext != null && ext.isEmpty() == false) {
                String normalizedExt = ext.toLowerCase(Locale.ROOT);
                // Ensure extension starts with a dot
                if (normalizedExt.startsWith(".") == false) {
                    normalizedExt = "." + normalizedExt;
                }
                byExtension.put(normalizedExt, supplier);
            }
        }
    }

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
                if (ext != null && ext.isEmpty() == false) {
                    String normalizedExt = ext.toLowerCase(Locale.ROOT);
                    // Ensure extension starts with a dot
                    if (normalizedExt.startsWith(".") == false) {
                        normalizedExt = "." + normalizedExt;
                    }
                    byExtension.put(normalizedExt, supplier);
                }
            }
        }
    }

    public Supplier<FormatReader> unregister(String formatName) {
        if (formatName == null || formatName.isEmpty()) {
            return null;
        }
        return byName.remove(formatName.toLowerCase(Locale.ROOT));
    }

    public FormatReader byName(String formatName) {
        if (formatName == null || formatName.isEmpty()) {
            throw new IllegalArgumentException("Format name cannot be null or empty");
        }

        Supplier<FormatReader> supplier = byName.get(formatName.toLowerCase(Locale.ROOT));
        if (supplier == null) {
            throw new IllegalArgumentException("No format reader registered for format: " + formatName);
        }
        return supplier.get();
    }

    public FormatReader byExtension(String objectName) {
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

    public boolean hasFormat(String formatName) {
        if (formatName == null || formatName.isEmpty()) {
            return false;
        }
        return byName.containsKey(formatName.toLowerCase(Locale.ROOT));
    }

    public boolean hasExtension(String extension) {
        if (extension == null || extension.isEmpty()) {
            return false;
        }
        String normalizedExt = extension.toLowerCase(Locale.ROOT);
        if (normalizedExt.startsWith(".") == false) {
            normalizedExt = "." + normalizedExt;
        }
        return byExtension.containsKey(normalizedExt);
    }
}
