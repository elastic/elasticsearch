/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.useragent.api.UserAgentParsedInfo;
import org.elasticsearch.useragent.api.UserAgentParserRegistry;
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
import java.util.SequencedMap;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Defines the `output` fields for {@code USER_AGENT}. {@link DocsV3Support} finds this class via
 * reflection by naming convention, then calls {@link #renderOutput} directly.
 */
public class UserAgentOutputFields {

    /**
     * Entry point called by {@link DocsV3Support.CommandsDocsSupport} via reflection. Delegates to
     * {@link OutputFields#renderFixedOutputBlock} with normal parameters.
     */
    public static void renderOutput(XContentBuilder builder) throws IOException {
        OutputFields.renderFixedOutputBlock(builder, allOutputFieldTypes());
    }

    /**
     * Returns the full set of possible output fields and their types, keyed by field name and sorted alphabetically.
     * Built by constructing a real {@link UserAgent} instance via {@link UserAgent#createInitialInstance} (the same
     * production entry point {@code LogicalPlanBuilder} uses) and reading the field names/types back off of it, so
     * this exercises the actual production conversion path instead of re-deriving it. Since USER_AGENT's field set
     * depends on the {@code properties}/{@code extract_device_type} options (filtered down by
     * {@code LogicalPlanBuilder} before calling {@code createInitialInstance}), docs want the unfiltered superset:
     * pass {@code extractDeviceType = true} and the full, unfiltered field map from
     * {@link UserAgentParsedInfo#getUserAgentInfoFields}.
     */
    public static SortedMap<String, DataType> allOutputFieldTypes() {
        Source source = Source.EMPTY;
        LogicalPlan child = new LocalRelation(Source.EMPTY, List.of(), null);
        Expression input = new ReferenceAttribute(Source.EMPTY, null, "input", DataType.KEYWORD, Nullability.TRUE, null, false);
        Attribute outputFieldPrefix = new ReferenceAttribute(Source.EMPTY, null, "prefix", DataType.KEYWORD, Nullability.TRUE, null, false);
        SequencedMap<String, Class<?>> allFields = UserAgentParsedInfo.getUserAgentInfoFields();
        UserAgent userAgent = UserAgent.createInitialInstance(
            source,
            child,
            input,
            outputFieldPrefix,
            true,
            UserAgentParserRegistry.DEFAULT_PARSER_NAME,
            allFields
        );

        SortedMap<String, DataType> result = new TreeMap<>();
        List<String> names = userAgent.outputFieldNames();
        List<Attribute> attrs = userAgent.generatedAttributes();
        for (int i = 0; i < names.size(); i++) {
            result.put(names.get(i), attrs.get(i).dataType());
        }
        return result;
    }
}
