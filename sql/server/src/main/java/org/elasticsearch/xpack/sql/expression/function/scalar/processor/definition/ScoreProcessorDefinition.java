/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition;

import org.elasticsearch.xpack.sql.execution.search.SqlSourceBuilder;
import org.elasticsearch.xpack.sql.execution.search.extractor.ScoreExtractor;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.runtime.Processor;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.runtime.HitExtractorProcessor;

import static java.util.Collections.emptyList;

public class ScoreProcessorDefinition extends ProcessorDefinition {

    public ScoreProcessorDefinition(Expression expression) {
        super(expression, emptyList());
    }

    @Override
    public boolean resolved() {
        return true;
    }

    @Override
    public Processor asProcessor() {
        return new HitExtractorProcessor(ScoreExtractor.INSTANCE);
    }

    @Override
    public void collectFields(SqlSourceBuilder sourceBuilder) {
        sourceBuilder.trackScores();
    }

    @Override
    public boolean supportedByAggsOnlyQuery() {
        return false;
    }
}
