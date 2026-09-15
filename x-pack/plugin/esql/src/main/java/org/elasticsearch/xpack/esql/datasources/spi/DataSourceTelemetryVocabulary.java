/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Closed telemetry dimension vocabulary shared by the ES|QL datasource query-path (APM) and
 * phone-home (XPack usage) sinks. Both sinks emit the same tokens so a dashboard can join a
 * query-path series onto a configuration-inventory series.
 * <p>
 * A local-filesystem source is {@code local} here in both sinks: the URI scheme is still
 * {@code file} ({@link StoragePath#scheme()}), but {@link Type#fromScheme(String)} folds it.
 * Out-of-vocabulary values clamp to {@link Type#UNKNOWN} — they are never passed through.
 */
public final class DataSourceTelemetryVocabulary {

    private DataSourceTelemetryVocabulary() {}

    /**
     * Storage type dimension. Values are the only tokens either sink may emit for this dimension.
     * <p>
     * {@link #fromTypeId(String)} maps a registered {@link DataSourceValidator#type()} identifier.
     * {@link #fromScheme(String)} maps a {@link StoragePath#scheme()} (and provider aliases), folding
     * {@code file} → {@link #LOCAL}. Neither derivation treats the other's input as interchangeable:
     * {@code fromTypeId("file")} and {@code fromScheme("local")} are both {@link #UNKNOWN}.
     */
    public enum Type {
        S3("s3", "s3", "s3a", "s3n"),
        GCS("gcs", "gs", "gcs"),
        AZURE("azure", "wasb", "wasbs", "azure"),
        HTTP("http", "http", "https"),
        LOCAL("local", "file"),
        UNKNOWN("unknown");

        private static final Map<String, Type> BY_KEY;
        private static final Map<String, Type> BY_SCHEME;

        static {
            Map<String, Type> byKey = new HashMap<>();
            Map<String, Type> byScheme = new HashMap<>();
            for (Type type : values()) {
                Type prevKey = byKey.put(type.key, type);
                assert prevKey == null : "duplicate type key [" + type.key + "]";
                for (String scheme : type.schemes) {
                    Type prevScheme = byScheme.put(scheme, type);
                    assert prevScheme == null : "duplicate scheme [" + scheme + "]";
                }
            }
            BY_KEY = Map.copyOf(byKey);
            BY_SCHEME = Map.copyOf(byScheme);
        }

        private final String key;
        private final Set<String> schemes;

        Type(String key, String... schemes) {
            this.key = key;
            this.schemes = Set.of(schemes);
        }

        /** Closed token emitted as an APM attribute value and as a phone-home key segment. */
        public String key() {
            return key;
        }

        /**
         * Maps a registered datasource type identifier ({@link DataSourceValidator#type()}) onto
         * this vocabulary. Null and anything other than a known Elastic type id become
         * {@link #UNKNOWN}. Scheme aliases ({@code s3a}, {@code file}, {@code gs}) are not type ids.
         */
        public static Type fromTypeId(String typeId) {
            if (typeId == null) {
                return UNKNOWN;
            }
            Type type = BY_KEY.get(typeId.toLowerCase(Locale.ROOT));
            return type != null ? type : UNKNOWN;
        }

        /**
         * Maps a URI scheme onto this vocabulary, folding provider aliases and {@code file} →
         * {@link #LOCAL}. Null and unrecognised schemes become {@link #UNKNOWN}.
         */
        public static Type fromScheme(String scheme) {
            if (scheme == null) {
                return UNKNOWN;
            }
            Type type = BY_SCHEME.get(scheme.toLowerCase(Locale.ROOT));
            return type != null ? type : UNKNOWN;
        }
    }
}
