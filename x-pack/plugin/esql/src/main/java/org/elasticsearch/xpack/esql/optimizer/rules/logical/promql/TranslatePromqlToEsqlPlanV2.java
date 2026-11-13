/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.promql;

import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.AnalyzerRules;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.TranslateTimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContextV2;

/**
 * Translates PromQL logical plan into ESQL plan like {@link TranslatePromqlToEsqlPlan}, carrying the labels of each series as
 * one label container instead of a column per label. Runs before {@link TranslateTimeSeriesAggregate} in its place when
 * {@code enable_promql_translation_v2} is set.
 * <p>
 * Mechanism: a {@link TranslationContextV2} per command; recursive descent via {@link TranslationContextV2#translate} where every
 * PromQL node produces an {@code IntermediateResult} its parent composes, and {@link TranslationContextV2#translateFinal}
 * projects the result to the command's declared output.
 */
public final class TranslatePromqlToEsqlPlanV2 extends AnalyzerRules.ParameterizedAnalyzerRule<PromqlCommand, AnalyzerContext> {

    @Override
    protected boolean skipResolved() {
        return false;
    }

    @Override
    protected LogicalPlan rule(PromqlCommand cmd, AnalyzerContext context) {
        return new TranslationContextV2(cmd, context).translateFinal();
    }
}
