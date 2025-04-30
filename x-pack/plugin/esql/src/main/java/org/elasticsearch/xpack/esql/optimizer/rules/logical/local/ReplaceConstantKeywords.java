/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.local;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.RegexExtract;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;

import java.util.HashMap;
import java.util.Map;

/**
 * Look for any constant_keyword fields used in the plan and replaces them with their actual value.
 */
public class ReplaceConstantKeywords extends ParameterizedRule<LogicalPlan, LogicalPlan, LocalLogicalOptimizerContext> {

    @Override
    public LogicalPlan apply(LogicalPlan plan, LocalLogicalOptimizerContext localLogicalOptimizerContext) {
        Map<Attribute, Expression> attrToValue = new HashMap<>();
        plan.forEachUp(EsRelation.class, esRelation -> {
            if (esRelation.indexMode() == IndexMode.STANDARD) {
                for (Attribute attribute : esRelation.output()) {
                    var val = localLogicalOptimizerContext.searchStats().constantValue(attribute.name());
                    if (val != null) {
                        attrToValue.put(attribute, Literal.of(attribute, val));
                    }
                }
            }
        });
        if (attrToValue.isEmpty()) {
            return plan;
        }
        return plan.transformUp(p -> replaceAttributes(p, attrToValue));
    }

    private LogicalPlan replaceAttributes(LogicalPlan plan, Map<Attribute, Expression> attrToValue) {
        // This is slightly different from ReplaceMissingFieldWithNull.
        // It's on purpose: reusing NameIDs is dangerous, and we have no evidence that adding an EVAL will actually lead to
        // practical performance benefits
        if (plan instanceof Eval
            || plan instanceof Filter
            || plan instanceof OrderBy
            || plan instanceof RegexExtract
            || plan instanceof TopN) {
            return plan.transformExpressionsOnlyUp(FieldAttribute.class, f -> attrToValue.getOrDefault(f, f));
        }

        return plan;
    }

}
