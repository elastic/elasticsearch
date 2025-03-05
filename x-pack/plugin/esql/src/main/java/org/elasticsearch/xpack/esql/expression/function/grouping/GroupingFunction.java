/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.grouping;

import org.elasticsearch.xpack.esql.capabilities.PostAnalysisPlanVerificationAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;
import java.util.function.BiConsumer;

import static org.elasticsearch.xpack.esql.common.Failure.fail;

public abstract class GroupingFunction extends Function implements EvaluatorMapper, PostAnalysisPlanVerificationAware {

    protected GroupingFunction(Source source, List<Expression> fields) {
        super(source, fields);
    }

    @Override
    public Object fold(FoldContext ctx) {
        return EvaluatorMapper.super.fold(source(), ctx);
    }

    @Override
    public BiConsumer<LogicalPlan, Failures> postAnalysisPlanVerification() {
        return (p, failures) -> {
            if (p instanceof Aggregate == false) {
                p.forEachExpression(
                    GroupingFunction.class,
                    gf -> failures.add(fail(gf, "cannot use grouping function [{}] outside of a STATS command", gf.sourceText()))
                );
            }
        };
    }

}
