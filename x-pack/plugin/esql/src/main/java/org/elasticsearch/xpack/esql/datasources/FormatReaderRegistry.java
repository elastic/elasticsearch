/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.Build;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.datasources.spi.DecompressionCodec;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReaderFactory;

import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Registry for FormatReader implementations, keyed by format name and file extension.
 * Readers are created lazily on first access to avoid pulling in heavy dependencies at startup.
 * Supports compound extensions (e.g. .csv.gz) via {@link DecompressionCodecRegistry}.
 */
public class FormatReaderRegistry {

    /**
     * Whole-file compression codecs supported for text formats on release builds, keyed by
     * {@link DecompressionCodec#name()}. {@code uncompressed} is the no-codec path and so is not listed.
     * On snapshot builds the gate in {@link #byExtension(String)} is bypassed, so any registered codec
     * resolves; the four codecs outside this set (bzip2, snappy, lz4, brotli) each return to the GA
     * surface once benchmarked (see elastic/esql-planning#938).
     */
    static final Set<String> GA_TEXT_CODECS = Set.of("gzip", "zstd");

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

    /**
     * Look up a format reader by name, returning null if not registered.
     * Use for speculative lookups where a missing format is normal (e.g., optimizer probing).
     */
    public FormatReader findByName(String formatName) {
        if (Strings.isNullOrEmpty(formatName)) {
            return null;
        }
        Supplier<FormatReader> supplier = byName.get(formatName.toLowerCase(Locale.ROOT));
        return supplier != null ? supplier.get() : null;
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

        String extension = trailingExtension(objectName);
        if (extension == null) {
            throw new IllegalArgumentException("Cannot infer format from object name without extension: " + objectName);
        }

        // Check for compound extension (e.g. .csv.gz)
        if (codecRegistry != null) {
            String stripped = codecRegistry.stripCompressionSuffix(objectName);
            if (stripped != null) {
                FormatReader inner = byExtension(stripped);
                DecompressionCodec codec = codecRegistry.byExtension(extension);
                if (codec != null) {
                    return wrapWithCodec(inner, codec, extension, objectName);
                }
            }
        }

        Supplier<FormatReader> supplier = byExtension.get(extension);
        Check.notNull(supplier, "No format reader registered for extension: {}. Supported: {}", extension, byExtension.keySet());
        return supplier.get();
    }

    /**
     * Resolves the named format reader and, if {@code objectName} carries a trailing compression-codec
     * extension (e.g. {@code .gz}), wraps it in a {@link CompressionDelegatingFormatReader} — applying the
     * same whole-file-compression-support check and GA-codec gate as {@link #byExtension(String)}.
     * <p>
     * Used when the caller already knows the format via an explicit {@code format}/{@code reader} config
     * override: without this, an explicit override would resolve the plain reader and feed it the resource's
     * compressed bytes unchanged (the compressed-read-under-explicit-format fix). {@code objectName} with no
     * compression suffix (the common case) returns the plain reader unchanged.
     */
    public FormatReader byNameForObject(String formatName, String objectName) {
        FormatReader inner = byName(formatName);
        if (codecRegistry == null || Strings.isNullOrEmpty(objectName)) {
            return inner;
        }
        String extension = trailingExtension(objectName);
        if (extension == null) {
            return inner;
        }
        DecompressionCodec codec = codecRegistry.byExtension(extension);
        if (codec == null) {
            return inner;
        }
        return wrapWithCodec(inner, codec, extension, objectName);
    }

    /**
     * Returns {@code objectName}'s trailing extension (e.g. {@code ".gz"}), lower-cased, or {@code null}
     * if there is no dot or the dot is the last character. Shared by {@link #byExtension(String)} and
     * {@link #byNameForObject(String, String)} so the two paths cannot diverge on how the compression
     * suffix is detected; callers decide separately whether a missing extension is an error.
     */
    private static String trailingExtension(String objectName) {
        int lastDot = objectName.lastIndexOf('.');
        if (lastDot < 0 || lastDot == objectName.length() - 1) {
            return null;
        }
        return objectName.substring(lastDot).toLowerCase(Locale.ROOT);
    }

    /**
     * Applies the whole-file-compression veto and the release-build GA-codec gate, then wraps {@code inner}
     * in a {@link CompressionDelegatingFormatReader} for {@code codec}. Shared by {@link #byExtension(String)}
     * (compound-extension inference) and {@link #byNameForObject(String, String)} (explicit format/reader
     * override), so the two paths cannot diverge on which codecs/formats are compatible.
     */
    private static FormatReader wrapWithCodec(FormatReader inner, DecompressionCodec codec, String extension, String objectName) {
        if (inner.supportsWholeFileCompression() == false) {
            throw new IllegalArgumentException(
                "Format ["
                    + inner.formatName()
                    + "] does not support whole-file compression; the ["
                    + extension
                    + "] suffix is not valid on ["
                    + objectName
                    + "]. Use an uncompressed file and rely on the format's built-in column compression instead."
            );
        }
        // On release builds the text-format codec surface is limited to the benchmarked set; the
        // remaining codecs (bzip2, snappy, lz4, brotli) stay available on snapshot builds only. This
        // runs after the whole-file veto so Parquet/ORC still report the more specific error above.
        if (Build.current().isSnapshot() == false && GA_TEXT_CODECS.contains(codec.name()) == false) {
            throw new IllegalArgumentException(
                "compression codec [" + codec.name() + "] is not supported; supported: uncompressed, gzip, zstd"
            );
        }
        return new CompressionDelegatingFormatReader(inner, codec);
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
