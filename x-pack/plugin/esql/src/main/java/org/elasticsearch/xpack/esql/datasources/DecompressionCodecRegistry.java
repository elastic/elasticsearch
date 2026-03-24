/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.datasources.spi.DecompressionCodec;

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for {@link DecompressionCodec} implementations, keyed by extension.
 * Populated from DataSourcePlugin.decompressionCodecs(Settings) at startup.
 */
public class DecompressionCodecRegistry {

    private final Map<String, DecompressionCodec> byExtension = new ConcurrentHashMap<>();

    /**
     * Register a codec and its extensions.
     */
    public void register(DecompressionCodec codec) {
        Check.notNull(codec, "Codec cannot be null");
        for (String ext : codec.extensions()) {
            if (Strings.isNullOrEmpty(ext) == false) {
                String normalized = ext.toLowerCase(Locale.ROOT);
                if (normalized.startsWith(".") == false) {
                    normalized = "." + normalized;
                }
                DecompressionCodec existing = byExtension.put(normalized, codec);
                if (existing != null && existing != codec) {
                    throw new IllegalArgumentException(
                        "Compression extension [" + normalized + "] already registered by codec [" + existing.name() + "]"
                    );
                }
            }
        }
    }

    /**
     * Look up a codec by extension (e.g. ".gz").
     */
    public DecompressionCodec byExtension(String extension) {
        if (Strings.isNullOrEmpty(extension)) {
            return null;
        }
        String normalized = extension.toLowerCase(Locale.ROOT);
        if (normalized.startsWith(".") == false) {
            normalized = "." + normalized;
        }
        return byExtension.get(normalized);
    }

    /**
     * Returns true if the given extension is a known compression extension.
     */
    public boolean hasCompressionExtension(String extension) {
        return byExtension(extension) != null;
    }

    /**
     * If the object name ends with a known compression extension, strips it and
     * returns the inner name (e.g. "data.csv.gz" -> "data.csv"). Otherwise returns null.
     */
    public String stripCompressionSuffix(String objectName) {
        if (Strings.isNullOrEmpty(objectName)) {
            return null;
        }
        int lastDot = objectName.lastIndexOf('.');
        if (lastDot < 0 || lastDot == objectName.length() - 1) {
            return null;
        }
        String extension = objectName.substring(lastDot).toLowerCase(Locale.ROOT);
        if (byExtension.containsKey(extension) == false) {
            return null;
        }
        return objectName.substring(0, lastDot);
    }
}
