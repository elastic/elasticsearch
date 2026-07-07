/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.docs.OutputFields;
import org.elasticsearch.xpack.esql.evaluator.command.UriPartsFunctionBridge;
import org.elasticsearch.xpack.esql.expression.function.DocsV3Support;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Defines the `output` fields for {@code URI_PARTS}. {@link DocsV3Support} finds this class via
 * reflection by naming convention, then calls {@link #renderOutput} directly.
 */
public class UriPartsOutputFields {

    /**
     * Entry point called by {@link DocsV3Support.CommandsDocsSupport} via reflection. Delegates to
     * {@link OutputFields#renderFixedOutputBlock} with normal parameters.
     */
    public static void renderOutput(XContentBuilder builder) throws IOException {
        OutputFields.renderFixedOutputBlock(builder, allOutputFieldTypes());
    }

    /**
     * Returns the full set of possible output fields and their types, keyed by field name and sorted alphabetically.
     */
    public static SortedMap<String, DataType> allOutputFieldTypes() {
        SortedMap<String, DataType> result = new TreeMap<>();
        for (var entry : UriPartsFunctionBridge.getAllOutputFields().entrySet()) {
            result.put(entry.getKey(), DataType.fromJavaType(entry.getValue()));
        }
        return result;
    }
}
