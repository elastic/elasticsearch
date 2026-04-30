/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;

/**
 * Controls how OTLP records are mapped to Elasticsearch documents.
 * <p>
 * Mirrors the {@code MappingMode} concept from the upstream Elasticsearch exporter in
 * opentelemetry-collector-contrib. The mode can be selected per-request via the
 * {@value #HEADER} HTTP header, or per-scope via the {@value #SCOPE_ATTRIBUTE}
 * instrumentation scope attribute (which takes precedence).
 */
public enum MappingMode {
    OTEL("otel"),
    BODYMAP("bodymap");

    public static final String HEADER = "X-Elastic-Mapping-Mode";
    public static final String SCOPE_ATTRIBUTE = "elastic.mapping.mode";

    private final String name;

    MappingMode(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    /**
     * Parses a mapping mode name. A {@code null} or empty value resolves to {@link #OTEL}.
     *
     * @throws IllegalArgumentException if {@code value} is non-empty and does not match a known mode
     */
    public static MappingMode parse(@Nullable String value) {
        if (Strings.hasLength(value) == false) {
            return OTEL;
        }
        for (MappingMode mode : values()) {
            if (mode.name.equalsIgnoreCase(value)) {
                return mode;
            }
        }
        throw new IllegalArgumentException("Unsupported mapping mode [" + value + "], expected one of [otel, bodymap]");
    }
}
