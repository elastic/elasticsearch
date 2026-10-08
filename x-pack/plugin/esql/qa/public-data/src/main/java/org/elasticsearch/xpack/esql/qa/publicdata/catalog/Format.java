/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.publicdata.catalog;

import java.util.List;
import java.util.Locale;

/** Physical data formats the matrix crosses. */
public enum Format {
    PARQUET("parquet", List.of(".parquet")),
    CSV("csv", List.of(".csv")),
    TSV("tsv", List.of(".tsv")),
    NDJSON("ndjson", List.of(".ndjson", ".json", ".jsonl"));

    private final String id;
    private final List<String> extensions;

    Format(String id, List<String> extensions) {
        this.id = id;
        this.extensions = extensions;
    }

    public String id() {
        return id;
    }

    /**
     * Whether {@code resource} (with any {@link Codec} suffix already stripped) plausibly carries
     * this format's file extension. Globs and extension-less resources are not checked.
     */
    public boolean matchesExtension(String resource) {
        return extensions.stream().anyMatch(resource::endsWith);
    }

    public static Format fromId(String id) {
        for (Format format : values()) {
            if (format.id.equals(id.toLowerCase(Locale.ROOT))) {
                return format;
            }
        }
        throw new IllegalArgumentException("Unknown format [" + id + "]; expected one of parquet, csv, tsv, ndjson");
    }
}
