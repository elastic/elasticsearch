/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.querydsl.agg;

import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.expression.gen.script.Scripts;
import org.elasticsearch.xpack.sql.util.Check;

import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.search.aggregations.PipelineAggregatorBuilders.bucketSelector;

public class AggFilter extends PipelineAgg {

    private static final String BUCKET_SELECTOR_ID_PREFIX = "having.";

    private final ScriptTemplate scriptTemplate;
    private final Map<String, String> aggPaths;

    public AggFilter(String name, ScriptTemplate scriptTemplate) {
        super(BUCKET_SELECTOR_ID_PREFIX + name);
        Check.isTrue(scriptTemplate != null, "a valid script is required");
        // make script null safe
        this.scriptTemplate = Scripts.nullSafeFilter(scriptTemplate);
        this.aggPaths = scriptTemplate.aggPaths();
    }

    public ScriptTemplate scriptTemplate() {
        return scriptTemplate;
    }

    @Override
    PipelineAggregationBuilder toBuilder() {
        Script script = scriptTemplate.toPainless();
        return bucketSelector(name(), aggPaths, script);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name(), scriptTemplate);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        AggFilter other = (AggFilter) obj;
        return Objects.equals(name(), other.name())
                && Objects.equals(scriptTemplate(), other.scriptTemplate());
    }

    @Override
    public String toString() {
        return scriptTemplate.toString();
    }
}
