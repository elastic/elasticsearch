/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.local;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.RegexExtract;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Look for any constant_keyword fields used in the plan and replaces them with their actual value.
 */
public class ReplaceConstantKeywords extends ParameterizedRule<LogicalPlan, LogicalPlan, LocalLogicalOptimizerContext> {

    @Override
    public LogicalPlan apply(LogicalPlan plan, LocalLogicalOptimizerContext localLogicalOptimizerContext) {
        Map<Attribute, Alias> attrToValue = new HashMap<>();
        plan.forEachUp(EsRelation.class, esRelation -> {
            if (esRelation.indexMode() == IndexMode.STANDARD) {
                for (Attribute attribute : esRelation.output()) {
                    var val = localLogicalOptimizerContext.searchStats().constantValue(attribute.name());
                    if (val != null) {
                        attrToValue.put(
                            attribute,
                            new Alias(attribute.source(), attribute.name(), Literal.of(attribute, val), attribute.id())
                        );
                    }
                }
            }
        });
        if (attrToValue.isEmpty()) {
            return plan;
        }
        return plan.transformUp(p -> replaceAttributes(p, attrToValue));
    }

    private LogicalPlan replaceAttributes(LogicalPlan plan, Map<Attribute, Alias> attrToValue) {
        if (plan instanceof EsRelation relation) {
            // For any missing field, place an Eval right after the EsRelation to assign constant values to that attribute (using the same
            // name
            // id!), thus avoiding that InsertFieldExtrations inserts a field extraction later.
            // This means that an EsRelation[field1, field2, field3] where field1 and field 3 are constants, will be replaced by
            // Project[field1, field2, field3] <- keeps the ordering intact
            // \_Eval[field1 = value, field3 = value]
            // \_EsRelation[field1, field2, field3]
            List<Attribute> relationOutput = relation.output();
            List<NamedExpression> newProjections = new ArrayList<>(relationOutput.size());
            for (int i = 0, size = relationOutput.size(); i < size; i++) {
                Attribute attr = relationOutput.get(i);
                Alias alias = attrToValue.get(attr);
                newProjections.add(alias == null ? attr : alias);
            }

            Eval eval = new Eval(plan.source(), relation, new ArrayList<>(attrToValue.values()));
            // This projection is redundant if there's another projection downstream (and no commands depend on the order until we hit it).
            return new Project(plan.source(), eval, newProjections.stream().map(NamedExpression::toAttribute).toList());
        }

        if (plan instanceof Eval
            || plan instanceof Filter
            || plan instanceof OrderBy
            || plan instanceof RegexExtract
            || plan instanceof TopN) {
            return plan.transformExpressionsOnlyUp(FieldAttribute.class, f -> {
                Alias alias = attrToValue.get(f);
                return alias != null ? alias.child() : f;
            });
        }

        return plan;
    }

}
