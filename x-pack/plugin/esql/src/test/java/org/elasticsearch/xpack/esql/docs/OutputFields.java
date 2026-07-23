/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.docs;

import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.SortedMap;

/**
 * Helpers for rendering command {@code output}-field blocks in Kibana docs.
 * These are called directly by the {@code *OutputFields} classes.
 */
public class OutputFields {

    private OutputFields() {}

    /**
     * Renders the output fields for one database variant into {@code builder}.
     */
    @FunctionalInterface
    public interface VariantFieldsRenderer<T> {
        void render(XContentBuilder builder, T variant) throws IOException;
    }

    /**
     * Renders the {@code vary_by: "none"} shape of the output block. Called directly by
     * {@code *OutputFields} classes via their {@code renderOutput(XContentBuilder)} method.
     */
    public static void renderFixedOutputBlock(XContentBuilder builder, SortedMap<String, DataType> outputFieldTypes) throws IOException {
        builder.startObject("output");
        builder.field("vary_by", "none");
        builder.startObject("variants");
        builder.startObject("all");
        for (Map.Entry<String, DataType> entry : outputFieldTypes.entrySet()) {
            builder.startObject(entry.getKey());
            builder.field("type", entry.getValue().esNameIfPossible());
            builder.endObject();
        }
        builder.endObject();
        builder.endObject();
        builder.endObject();
    }

    /**
     * Renders a "varies by something" shape of the output block: a keyed set of named variants, each rendering its
     * own fields. For example, IP_LOCATION uses this with {@code varyBy = "database_file"} and
     * {@code selectedBy = "properties"}, keying variants by database file name glob (see
     * {@code IpLocationOutputFields}). Called directly by {@code *OutputFields} classes via their
     * {@code renderOutput(XContentBuilder)} method.
     *
     * @param varyBy      the {@code vary_by} value, describing what selects between variants
     * @param selectedBy  the {@code selected_by} value, describing what narrows the fields within a variant;
     *                    omitted entirely (no {@code selected_by} field rendered) when {@code null}
     * @param variants    the variants, keyed by the value used to select them, in the order they should be rendered
     * @param renderer    renders one variant's fields
     */
    public static <T> void renderVariedOutputBlock(
        XContentBuilder builder,
        String varyBy,
        String selectedBy,
        LinkedHashMap<String, T> variants,
        VariantFieldsRenderer<T> renderer
    ) throws IOException {
        builder.startObject("output");
        builder.field("vary_by", varyBy);
        if (selectedBy != null) {
            builder.field("selected_by", selectedBy);
        }
        builder.startObject("variants");
        for (Map.Entry<String, T> entry : variants.entrySet()) {
            builder.startObject(entry.getKey());
            renderer.render(builder, entry.getValue());
            builder.endObject();
        }
        builder.endObject();
        builder.endObject();
    }
}
