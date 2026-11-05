/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Describes and writes an object that is part of the ES|QL profile response.
 *
 * <p>The schema representation deliberately targets JSON Schema draft 7. It only exposes
 * the small set of constructs needed by profile objects so generated schemas stay predictable
 * for clients that generate source types from them.
 */
public final class ProfileSchema<T> {

    /** Writes either a complete object or an inline object fragment. */
    @FunctionalInterface
    public interface Writer<T> {
        XContentBuilder write(XContentBuilder builder, T value, ToXContent.Params params) throws IOException;
    }

    /** Produces chunks for a complete object or inline object fragment. */
    @FunctionalInterface
    public interface ChunkedWriter<T> {
        Iterator<? extends ToXContent> write(T value, ToXContent.Params params);
    }

    private final Map<String, Object> schema;
    private final Class<T> javaType;
    private final Writer<T> writer;
    private final ChunkedWriter<T> chunkedWriter;

    private ProfileSchema(Map<String, Object> schema, Class<T> javaType, Writer<T> writer, ChunkedWriter<T> chunkedWriter) {
        this.schema = Collections.unmodifiableMap(new LinkedHashMap<>(schema));
        this.javaType = Objects.requireNonNull(javaType);
        this.writer = writer;
        this.chunkedWriter = chunkedWriter;
    }

    /** Build a strict schema for an object of {@code javaType}. */
    public static <T> Builder<T> of(Class<T> javaType, Writer<T> writer) {
        return new Builder<>(javaType, Objects.requireNonNull(writer), null, false);
    }

    /** Build a strict schema for an object whose production writer is chunked. */
    public static <T> Builder<T> chunked(Class<T> javaType, ChunkedWriter<T> writer) {
        return new Builder<>(javaType, null, Objects.requireNonNull(writer), false);
    }

    /** Build an object fragment intended to be inlined into a containing object. */
    public static <T> Builder<T> fragment(Class<T> javaType, Writer<T> writer) {
        return new Builder<>(javaType, Objects.requireNonNull(writer), null, true);
    }

    /** The draft-7 schema for this object or fragment. */
    public Map<String, Object> schema() {
        return schema;
    }

    /** The Java value described and written by this schema. */
    public Class<T> javaType() {
        return javaType;
    }

    /** Write the object or fragment using the writer colocated with its schema. */
    public XContentBuilder write(XContentBuilder builder, T value, ToXContent.Params params) throws IOException {
        if (writer != null) {
            return writer.write(builder, value, params);
        }
        Iterator<? extends ToXContent> chunks = chunkedWriter.write(value, params);
        while (chunks.hasNext()) {
            chunks.next().toXContent(builder, params);
        }
        return builder;
    }

    /** Produce the same serialization as {@link #write}, preserving chunks when available. */
    public Iterator<? extends ToXContent> toXContentChunked(T value, ToXContent.Params params) {
        if (chunkedWriter != null) {
            return chunkedWriter.write(value, params);
        }
        return List.<ToXContent>of((builder, chunkParams) -> writer.write(builder, value, chunkParams)).iterator();
    }

    /** A string schema. */
    public static Map<String, Object> string() {
        return type("string");
    }

    /** An integer schema. */
    public static Map<String, Object> integer() {
        return type("integer");
    }

    /** A numeric schema. */
    public static Map<String, Object> number() {
        return type("number");
    }

    /** A boolean schema. */
    public static Map<String, Object> bool() {
        return type("boolean");
    }

    /** A null schema. */
    public static Map<String, Object> nullValue() {
        return type("null");
    }

    /** A local definition reference. */
    public static Map<String, Object> ref(String definition) {
        return Map.of("$ref", "#/definitions/" + definition);
    }

    /** A schema restricted to one value. */
    public static Map<String, Object> constant(Object value) {
        return Map.of("const", value);
    }

    /** A schema restricted to the supplied values. */
    public static Map<String, Object> enumeration(Object... values) {
        return Map.of("enum", List.of(values));
    }

    /** An array schema. */
    public static Map<String, Object> array(Map<String, Object> items) {
        return orderedMap("type", "array", "items", items);
    }

    /** An object with arbitrary property names and values of the supplied schema. */
    public static Map<String, Object> dynamicObject(Map<String, Object> values) {
        return orderedMap("type", "object", "additionalProperties", values);
    }

    /** A schema accepting any supplied alternative. */
    @SafeVarargs
    @SuppressWarnings("varargs")
    public static Map<String, Object> anyOf(Map<String, Object>... alternatives) {
        return Map.of("anyOf", List.copyOf(Arrays.asList(alternatives)));
    }

    /** A schema accepting exactly one supplied alternative. */
    @SafeVarargs
    @SuppressWarnings("varargs")
    public static Map<String, Object> oneOf(Map<String, Object>... alternatives) {
        return Map.of("oneOf", List.copyOf(Arrays.asList(alternatives)));
    }

    /** A schema accepting either the supplied value or null. */
    public static Map<String, Object> nullable(Map<String, Object> value) {
        return anyOf(value, nullValue());
    }

    private static Map<String, Object> type(String type) {
        return Map.of("type", type);
    }

    private static Map<String, Object> orderedMap(Object... entries) {
        Map<String, Object> result = new LinkedHashMap<>();
        for (int i = 0; i < entries.length; i += 2) {
            result.put((String) entries[i], entries[i + 1]);
        }
        return result;
    }

    /** Builds the ordered properties and required list for an object or inline fragment. */
    public static final class Builder<T> {
        private final Class<T> javaType;
        private final Writer<T> writer;
        private final ChunkedWriter<T> chunkedWriter;
        private final boolean fragment;
        private final Map<String, Object> properties = new LinkedHashMap<>();
        private final List<String> required = new ArrayList<>();

        private Builder(Class<T> javaType, Writer<T> writer, ChunkedWriter<T> chunkedWriter, boolean fragment) {
            this.javaType = Objects.requireNonNull(javaType);
            this.writer = writer;
            this.chunkedWriter = chunkedWriter;
            this.fragment = fragment;
        }

        /** Add a required property. */
        public Builder<T> required(String name, Map<String, Object> propertySchema) {
            property(name, propertySchema);
            required.add(name);
            return this;
        }

        /** Add an optional property. */
        public Builder<T> optional(String name, Map<String, Object> propertySchema) {
            property(name, propertySchema);
            return this;
        }

        /** Mark previously declared properties as required. */
        public Builder<T> requiredAll(Collection<String> names) {
            required.addAll(names);
            return this;
        }

        /**
         * Inline another descriptor's properties and required fields. Its writer is not invoked;
         * callers compose writers explicitly to preserve the containing object's field order.
         */
        public Builder<T> include(ProfileSchema<?> other) {
            return include(other, true);
        }

        /** Inline another descriptor, optionally carrying its required fields into this object. */
        @SuppressWarnings("unchecked")
        public Builder<T> include(ProfileSchema<?> other, boolean includeRequired) {
            Object otherProperties = other.schema.get("properties");
            if (otherProperties instanceof Map<?, ?> map) {
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    property((String) entry.getKey(), (Map<String, Object>) entry.getValue());
                }
            }
            if (includeRequired) {
                Object otherRequired = other.schema.get("required");
                if (otherRequired instanceof Collection<?> collection) {
                    for (Object name : collection) {
                        required.add((String) name);
                    }
                }
            }
            return this;
        }

        /** Build the immutable descriptor. */
        public ProfileSchema<T> build() {
            Map<String, Object> result = new LinkedHashMap<>();
            if (fragment == false) {
                result.put("type", "object");
            }
            result.put("properties", Collections.unmodifiableMap(new LinkedHashMap<>(properties)));
            result.put("required", List.copyOf(required));
            if (fragment == false) {
                result.put("additionalProperties", false);
            }
            return new ProfileSchema<>(result, javaType, writer, chunkedWriter);
        }

        private void property(String name, Map<String, Object> propertySchema) {
            if (properties.putIfAbsent(name, Collections.unmodifiableMap(new LinkedHashMap<>(propertySchema))) != null) {
                throw new IllegalArgumentException("duplicate profile property [" + name + "]");
            }
        }
    }

    /**
     * A polymorphic profile variant whose wire registration and JSON schema share one stable name.
     */
    public record Variant<T extends NamedWriteable>(
        NamedWriteableRegistry.Entry namedWriteableEntry,
        ProfileSchema<? extends T> profileSchema
    ) {
        public Variant {
            Objects.requireNonNull(namedWriteableEntry);
            Objects.requireNonNull(profileSchema);
        }

        /** The stable name shared by the wire registration and profile schema. */
        public String name() {
            return namedWriteableEntry.name;
        }

        /** Pair an existing wire entry with its profile schema. */
        public static <T extends NamedWriteable> Variant<T> fromEntry(
            Class<T> categoryClass,
            NamedWriteableRegistry.Entry entry,
            ProfileSchema<? extends T> profileSchema
        ) {
            if (entry.categoryClass != categoryClass) {
                throw new IllegalArgumentException(
                    "wire entry category [" + entry.categoryClass.getName() + "] does not match [" + categoryClass.getName() + "]"
                );
            }
            if (categoryClass.isAssignableFrom(profileSchema.javaType()) == false) {
                throw new IllegalArgumentException(
                    "profile type [" + profileSchema.javaType().getName() + "] does not implement [" + categoryClass.getName() + "]"
                );
            }
            return new Variant<>(entry, profileSchema);
        }
    }
}
