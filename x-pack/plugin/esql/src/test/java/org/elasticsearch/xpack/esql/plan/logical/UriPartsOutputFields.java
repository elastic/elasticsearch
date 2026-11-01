/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.docs.OutputFields;
import org.elasticsearch.xpack.esql.expression.function.DocsV3Support;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.io.IOException;
import java.util.List;
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
     * Built by constructing a real {@link UriParts} instance via {@link UriParts#createInitialInstance} (the same
     * production entry point {@code LogicalPlanBuilder} uses) and reading the field names/types back off of it, so
     * this exercises the actual production conversion path instead of re-deriving it.
     */
    public static SortedMap<String, DataType> allOutputFieldTypes() {
        Source source = Source.EMPTY;
        LogicalPlan child = new LocalRelation(Source.EMPTY, List.of(), null);
        Expression input = new ReferenceAttribute(Source.EMPTY, null, "input", DataType.KEYWORD, Nullability.TRUE, null, false);
        Attribute outputFieldPrefix = new ReferenceAttribute(Source.EMPTY, null, "prefix", DataType.KEYWORD, Nullability.TRUE, null, false);
        UriParts uriParts = UriParts.createInitialInstance(source, child, input, outputFieldPrefix);

        SortedMap<String, DataType> result = new TreeMap<>();
        List<String> names = uriParts.outputFieldNames();
        List<Attribute> attrs = uriParts.generatedAttributes();
        for (int i = 0; i < names.size(); i++) {
            result.put(names.get(i), attrs.get(i).dataType());
        }
        return result;
    }
}
