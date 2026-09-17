/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.publicdata.catalog;

import java.util.List;
import java.util.Locale;

/** Compression codecs the matrix crosses. */
public enum Codec {
    UNCOMPRESSED("uncompressed", List.of()),
    /** Typically internal to Parquet (per-page), not a file suffix. */
    SNAPPY("snappy", List.of(".snappy")),
    ZSTD("zstd", List.of(".zst", ".zstd")),
    GZIP("gzip", List.of(".gz", ".gzip"));

    private final String id;
    private final List<String> extensions;

    Codec(String id, List<String> extensions) {
        this.id = id;
        this.extensions = extensions;
    }

    public String id() {
        return id;
    }

    /** Whether {@code resource} ends with one of this codec's file suffixes. */
    public boolean matchesExtension(String resource) {
        return extensions.stream().anyMatch(resource::endsWith);
    }

    /** Strips this codec's suffix from {@code resource}, if present, for {@link Format} checks. */
    public String stripExtension(String resource) {
        for (String extension : extensions) {
            if (resource.endsWith(extension)) {
                return resource.substring(0, resource.length() - extension.length());
            }
        }
        return resource;
    }

    public static Codec fromId(String id) {
        for (Codec codec : values()) {
            if (codec.id.equals(id.toLowerCase(Locale.ROOT))) {
                return codec;
            }
        }
        throw new IllegalArgumentException("Unknown codec [" + id + "]; expected one of uncompressed, snappy, zstd, gzip");
    }
}
