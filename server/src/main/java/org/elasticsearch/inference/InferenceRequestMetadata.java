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
import org.elasticsearch.tasks.Task;

import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Immutable attribution carried with an inference request and forwarded to Elastic Inference Service.
 * Missing entries are absent. Null and empty values are not stored.
 * <p>
 * Product origin is a field, but core owns its propagation: core registers its REST header and preserves it
 * across context stashes. Only {@link Field#INFERENCE_PROPAGATED} fields are registered, carried, and restored
 * by the inference plugin.
 */
public final class InferenceRequestMetadata {

    /**
     * A supported request-metadata field. Declaration order is not a transport layout.
     */
    public enum Field {
        PRODUCT_ORIGIN(Task.X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER, "product_origin", false, false),
        PRODUCT_USE_CASE("X-elastic-product-use-case", "product_use_case", true, true),
        PRODUCT_SOLUTION("X-elastic-product-solution", "product_solution", false, true),
        PRODUCT_FEATURE("X-elastic-product-feature", "product_feature", false, true),
        INTERACTION_ID("X-Elastic-Inference-Interaction-Id", "interaction_id", false, true),
        TRACE_ID("X-Elastic-Trace-Id", "trace_id", false, true),
        USER_ID("X-Elastic-User-Id", "user_id", false, true),
        SPACE_ID("X-Elastic-Space-Id", "space_id", false, true);

        /**
         * Fields whose REST header registration, request-payload transport, and thread-context restoration
         * the inference plugin owns.
         */
        public static final Set<Field> INFERENCE_PROPAGATED;

        static {
            var propagated = EnumSet.noneOf(Field.class);
            for (var field : values()) {
                if (field.propagatedByInference) {
                    propagated.add(field);
                }
            }
            INFERENCE_PROPAGATED = Collections.unmodifiableSet(propagated);
        }

        private final String httpHeader;
        private final String xContentName;
        private final boolean allowsMultipleRestValues;
        private final boolean propagatedByInference;

        Field(String httpHeader, String xContentName, boolean allowsMultipleRestValues, boolean propagatedByInference) {
            this.httpHeader = httpHeader;
            this.xContentName = xContentName;
            this.allowsMultipleRestValues = allowsMultipleRestValues;
            this.propagatedByInference = propagatedByInference;
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

        public boolean propagatedByInference() {
            return propagatedByInference;
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
     * Reads every supported header, including product origin, through {@code headerLookup}. Null and empty results are omitted.
     */
    public static InferenceRequestMetadata capture(Function<String, String> headerLookup) {
        return capture(EnumSet.allOf(Field.class), headerLookup);
    }

    /**
     * Reads only the headers of {@code fields} through {@code headerLookup}. Null and empty results are omitted.
     */
    public static InferenceRequestMetadata capture(Set<Field> fields, Function<String, String> headerLookup) {
        Objects.requireNonNull(fields);
        Objects.requireNonNull(headerLookup);
        var builder = builder();
        for (var field : fields) {
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
