/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.agg;

import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.xpack.sql.expression.function.scalar.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.querydsl.container.Sort.Direction;

import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

public class GroupByScriptAgg extends GroupByColumnAgg {

    private final ScriptTemplate script;

    public GroupByScriptAgg(String id, String propertyPath, String fieldName, ScriptTemplate script) {
        this(id, propertyPath, fieldName, script, emptyList(), emptyList(), emptyMap());
    }

    public GroupByScriptAgg(String id, String propertyPath, String fieldName, ScriptTemplate script, List<LeafAgg> subAggs,
            List<PipelineAgg> subPipelines, Map<String, Direction> order) {
        super(id, propertyPath, fieldName, subAggs, subPipelines, order);
        this.script = script;
    }

    public ScriptTemplate script() {
        return script;
    }

    @Override
    protected TermsAggregationBuilder termsTarget(TermsAggregationBuilder builder) {
        builder.script(script.toPainless());
        if (script.outputType().isNumeric()) {
            builder.valueType(ValueType.NUMBER);
        }

        return builder;
    }

    @Override
    protected GroupByScriptAgg copy(String id, String propertyPath, String fieldName, List<LeafAgg> subAggs, List<PipelineAgg> subPipelines, Map<String, Direction> order) {
        return new GroupByScriptAgg(id, propertyPath, fieldName, script, subAggs, subPipelines, order);
    }
}