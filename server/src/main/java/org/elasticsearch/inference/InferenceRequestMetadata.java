/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.core.Nullable;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Immutable attribution carried with an inference request and forwarded to Elastic Inference Service.
 * Missing entries are absent. Null and empty values are not stored.
 * <p>
 * Product origin is intentionally not a field. It has its own header and propagation path.
 */
public final class InferenceRequestMetadata {

    /**
     * A supported request-metadata field. Declaration order is not a transport layout.
     */
    public enum Field {
        PRODUCT_USE_CASE("X-elastic-product-use-case", "product_use_case", true),
        PRODUCT_SOLUTION("X-elastic-product-solution", "product_solution", false),
        PRODUCT_FEATURE("X-elastic-product-feature", "product_feature", false),
        INTERACTION_ID("X-Elastic-Inference-Interaction-Id", "interaction_id", false);

        private final String httpHeader;
        private final String xContentName;
        private final boolean allowsMultipleRestValues;

        Field(String httpHeader, String xContentName, boolean allowsMultipleRestValues) {
            this.httpHeader = httpHeader;
            this.xContentName = xContentName;
            this.allowsMultipleRestValues = allowsMultipleRestValues;
        }

        public String httpHeader() {
            return httpHeader;
        }

        public String xContentName() {
            return xContentName;
        }

        public boolean allowsMultipleRestValues() {
            return allowsMultipleRestValues;
        }
    }

    public static final InferenceRequestMetadata EMPTY = new InferenceRequestMetadata(Map.of());

    private final Map<Field, String> values;

    private InferenceRequestMetadata(Map<Field, String> values) {
        this.values = values;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Reads each supported header through {@code headerLookup}. Null and empty results are omitted.
     */
    public static InferenceRequestMetadata capture(Function<String, String> headerLookup) {
        Objects.requireNonNull(headerLookup);
        var builder = builder();
        for (var field : Field.values()) {
            builder.put(field, headerLookup.apply(field.httpHeader()));
        }
        return builder.build();
    }

    /**
     * @return the stored value, or {@code null} when the field is absent
     */
    @Nullable
    public String get(Field field) {
        Objects.requireNonNull(field);
        return values.get(field);
    }

    /**
     * Visits stored values directly. This does not copy them into another map.
     */
    public void forEachPresent(BiConsumer<Field, String> consumer) {
        Objects.requireNonNull(consumer);
        for (var entry : values.entrySet()) {
            consumer.accept(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return values.equals(((InferenceRequestMetadata) o).values);
    }

    @Override
    public int hashCode() {
        return values.hashCode();
    }

    @Override
    public String toString() {
        return values.toString();
    }

    public static final class Builder {
        private final EnumMap<Field, String> values = new EnumMap<>(Field.class);

        public Builder put(Field field, @Nullable String value) {
            Objects.requireNonNull(field);
            if (value == null || value.isEmpty()) {
                values.remove(field);
            } else {
                values.put(field, value);
            }
            return this;
        }

        public InferenceRequestMetadata build() {
            if (values.isEmpty()) {
                return EMPTY;
            }
            return new InferenceRequestMetadata(Collections.unmodifiableMap(new EnumMap<>(values)));
        }
    }
}
