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
     * Renders the {@code vary_by: "database_file"} shape of the output block (currently only
     * IP_LOCATION). Called directly by {@code IpLocationOutputFields} via its
     * {@code renderOutput(XContentBuilder)} method. Matching is filename-based on known glob patterns;
     * a custom database registered under a standard name would get incorrect autocomplete hints in
     * Kibana, but ES itself resolves fields from the actual database metadata, so queries are
     * unaffected.
     */
    public static <T> void renderDatabaseFileOutputBlock(
        XContentBuilder builder,
        LinkedHashMap<String, T> databaseGlobs,
        VariantFieldsRenderer<T> renderer
    ) throws IOException {
        builder.startObject("output");
        builder.field("vary_by", "database_file");
        builder.field("selected_by", "properties");
        builder.startObject("variants");
        for (Map.Entry<String, T> entry : databaseGlobs.entrySet()) {
            builder.startObject(entry.getKey());
            renderer.render(builder, entry.getValue());
            builder.endObject();
        }
        builder.endObject();
        builder.endObject();
    }
}
