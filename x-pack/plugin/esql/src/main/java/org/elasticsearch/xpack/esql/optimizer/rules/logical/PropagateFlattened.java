/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.type.FlattenedEsField;
import org.elasticsearch.xpack.esql.core.type.KeywordEsField;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedKeywordEsField;
import org.elasticsearch.xpack.esql.expression.NamedExpressions;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * Merges synthetic dotted subfields under {@link FlattenedEsField} roots into {@link EsRelation} output so shard block loaders
 * include keyed flattened columns produced by {@link ReplaceFlattenedJsonExtractFn}.
 */
public final class PropagateFlattened extends Rule<LogicalPlan, LogicalPlan> {

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        if (EsqlCapabilities.Cap.JSON_EXTRACT_FLATTENED_FIELD.isEnabled() == false) {
            return plan;
        }
        return plan.transformUp(EsRelation.class, er -> propagate(er, plan));
    }

    private static EsRelation propagate(EsRelation er, LogicalPlan fullPlan) {
        Set<String> flattenedRoots = new HashSet<>();
        for (Attribute a : er.output()) {
            if (a instanceof FieldAttribute fa && fa.field() instanceof FlattenedEsField) {
                flattenedRoots.add(fa.name());
            }
        }
        if (flattenedRoots.isEmpty()) {
            return er;
        }
        AttributeSet.Builder extras = AttributeSet.builder();
        fullPlan.forEachExpressionDown(FieldAttribute.class, fa -> {
            if (fa.field() instanceof FlattenedEsField) {
                return;
            }
            if (fa.field() instanceof KeywordEsField == false && fa.field() instanceof PotentiallyUnmappedKeywordEsField == false) {
                return;
            }
            for (String root : flattenedRoots) {
                String prefix = root + ".";
                if (fa.name().startsWith(prefix) && fa.name().length() > prefix.length()) {
                    extras.add(fa);
                    break;
                }
            }
        });
        var set = extras.build();
        return set.isEmpty() ? er : er.withAttributes(NamedExpressions.mergeOutputAttributes(new ArrayList<>(set), er.output()));
    }
}
