/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedKeywordEsField;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Pushes {@link PotentiallyUnmappedKeywordEsField} attributes down into {@link EsRelation} outputs so block
 * loaders know to fetch from {@code _source}. Only those not yet declared in any {@link EsRelation} output
 * are propagated — the gate is structural, not command-specific, so any future feature that introduces
 * them above a relation is handled automatically. Today that is only {@code INSIST}.
 * <p>
 * N.B. When INSIST is sunset, this rule can be removed.
 */
public class PropagateUnmappedFields extends Rule<LogicalPlan, LogicalPlan> {
    @Override
    public LogicalPlan apply(LogicalPlan logicalPlan) {
        if (logicalPlan instanceof EsRelation) {
            return logicalPlan;
        }
        var fields = collectAttributesToPropagate(logicalPlan);
        return fields.isEmpty()
            ? logicalPlan
            : logicalPlan.transformUp(
                EsRelation.class,
                er -> er.indexMode() == IndexMode.LOOKUP ? er : er.withAdditionalAttributes(fields)
            );
    }

    private static List<Attribute> collectAttributesToPropagate(LogicalPlan plan) {
        Set<String> alreadyPropagated = new HashSet<>();
        plan.forEachDown(EsRelation.class, er -> {
            for (Attribute a : er.output()) {
                if (a instanceof FieldAttribute fa && fa.field() instanceof PotentiallyUnmappedKeywordEsField) {
                    alreadyPropagated.add(fa.fieldName().string());
                }
            }
        });
        var builder = AttributeSet.builder();
        plan.forEachExpressionDown(FieldAttribute.class, fa -> {
            if (fa.field() instanceof PotentiallyUnmappedKeywordEsField && alreadyPropagated.contains(fa.fieldName().string()) == false) {
                builder.add(fa);
            }
        });
        return new ArrayList<>(builder.build());
    }
}
