/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.datasources.spi.DecompressionCodec;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReaderFactory;

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Registry for FormatReader implementations, keyed by format name and file extension.
 * Readers are created lazily on first access to avoid pulling in heavy dependencies at startup.
 * Supports compound extensions (e.g. .csv.gz) via {@link DecompressionCodecRegistry}.
 */
public class FormatReaderRegistry {

    private final Map<String, Supplier<FormatReader>> byName = new ConcurrentHashMap<>();
    private final Map<String, Supplier<FormatReader>> byExtension = new ConcurrentHashMap<>();
    private final DecompressionCodecRegistry codecRegistry;

    public FormatReaderRegistry(DecompressionCodecRegistry codecRegistry) {
        this.codecRegistry = codecRegistry;
    }

    public void registerLazy(String formatName, FormatReaderFactory factory, Settings settings, BlockFactory blockFactory) {
        if (Strings.isNullOrEmpty(formatName)) {
            throw new IllegalArgumentException("Format name cannot be null or empty");
        }
        Check.notNull(factory, "Factory cannot be null");

        // Lazy supplier that creates the reader on first access and registers extensions
        Supplier<FormatReader> lazySupplier = new Supplier<>() {
            private volatile FormatReader instance;

            @Override
            public FormatReader get() {
                if (instance == null) {
                    synchronized (this) {
                        if (instance == null) {
                            instance = factory.create(settings, blockFactory);
                            // Register extension mappings now that the reader is created
                            for (String ext : instance.fileExtensions()) {
                                if (Strings.isNullOrEmpty(ext) == false) {
                                    String normalizedExt = ext.toLowerCase(Locale.ROOT);
                                    if (normalizedExt.startsWith(".") == false) {
                                        normalizedExt = "." + normalizedExt;
                                    }
                                    byExtension.put(normalizedExt, this);
                                }
                            }
                        }
                    }
                }
                return instance;
            }
        };

        byName.put(formatName.toLowerCase(Locale.ROOT), lazySupplier);
    }

    public Supplier<FormatReader> unregister(String formatName) {
        if (Strings.isNullOrEmpty(formatName)) {
            return null;
        }
        return byName.remove(formatName.toLowerCase(Locale.ROOT));
    }

    public FormatReader byName(String formatName) {
        if (Strings.isNullOrEmpty(formatName)) {
            throw new IllegalArgumentException("Format name cannot be null or empty");
        }

        Supplier<FormatReader> supplier = byName.get(formatName.toLowerCase(Locale.ROOT));
        Check.notNull(supplier, "No format reader registered for format: " + formatName);
        return supplier.get();
    }

    public void registerExtension(String extension, String formatName) {
        String normalizedExt = extension.toLowerCase(Locale.ROOT);
        if (normalizedExt.startsWith(".") == false) {
            normalizedExt = "." + normalizedExt;
        }
        Supplier<FormatReader> supplier = byName.get(formatName.toLowerCase(Locale.ROOT));
        Check.notNull(supplier, "Cannot register extension [{}] -- format [{}] not registered", extension, formatName);
        byExtension.put(normalizedExt, supplier);
    }

    public FormatReader byExtension(String objectName) {
        if (Strings.isNullOrEmpty(objectName)) {
            throw new IllegalArgumentException("Object name cannot be null or empty");
        }

        int lastDot = objectName.lastIndexOf('.');
        if (lastDot < 0 || lastDot == objectName.length() - 1) {
            throw new IllegalArgumentException("Cannot infer format from object name without extension: " + objectName);
        }

        String extension = objectName.substring(lastDot).toLowerCase(Locale.ROOT);

        // Check for compound extension (e.g. .csv.gz)
        if (codecRegistry != null) {
            String stripped = codecRegistry.stripCompressionSuffix(objectName);
            if (stripped != null) {
                FormatReader inner = byExtension(stripped);
                DecompressionCodec codec = codecRegistry.byExtension(extension);
                if (codec != null) {
                    return new CompressionDelegatingFormatReader(inner, codec);
                }
            }
        }

        Supplier<FormatReader> supplier = byExtension.get(extension);
        Check.notNull(supplier, "No format reader registered for extension: {}. Supported: {}", extension, byExtension.keySet());
        return supplier.get();
    }

    /**
     * Returns true if the object name has a compound extension (e.g. .csv.gz) that is supported:
     * the last extension is a known compression extension and the stripped path has a format.
     */
    public boolean hasCompressedExtension(String objectName) {
        if (Strings.isNullOrEmpty(objectName) || codecRegistry == null) {
            return false;
        }
        String stripped = codecRegistry.stripCompressionSuffix(objectName);
        if (stripped == null) {
            return false;
        }
        int innerDot = stripped.lastIndexOf('.');
        if (innerDot < 0 || innerDot == stripped.length() - 1) {
            return false;
        }
        String innerExt = stripped.substring(innerDot).toLowerCase(Locale.ROOT);
        return byExtension.containsKey(innerExt);
    }

    public boolean hasFormat(String formatName) {
        if (Strings.isNullOrEmpty(formatName)) {
            return false;
        }
        return byName.containsKey(formatName.toLowerCase(Locale.ROOT));
    }

    public boolean hasExtension(String extension) {
        if (Strings.isNullOrEmpty(extension)) {
            return false;
        }
        String normalizedExt = extension.toLowerCase(Locale.ROOT);
        if (normalizedExt.startsWith(".") == false) {
            normalizedExt = "." + normalizedExt;
        }
        return byExtension.containsKey(normalizedExt);
    }
}
