/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedKeywordEsField;
import org.elasticsearch.xpack.esql.expression.NamedExpressions;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.ArrayList;

/**
 * Merges unmapped fields into the output of the ES relation. This marking is necessary for the block loaders to force loading from _source
 * if the field is unmapped.
 */
public class PropgateUnmappedFields extends Rule<LogicalPlan, LogicalPlan> {
    @Override
    public LogicalPlan apply(LogicalPlan logicalPlan) {
        if (logicalPlan instanceof EsRelation) {
            return logicalPlan;
        }
        var unmappedFieldsBuilder = AttributeSet.builder();
        logicalPlan.forEachExpressionDown(FieldAttribute.class, fa -> {
            if (fa.field() instanceof PotentiallyUnmappedKeywordEsField) {
                unmappedFieldsBuilder.add(fa);
            }
        });
        var unmappedFields = unmappedFieldsBuilder.build();
        return unmappedFields.isEmpty()
            ? logicalPlan
            : logicalPlan.transformUp(
                EsRelation.class,
                er -> er.withAttributes(NamedExpressions.mergeOutputAttributes(new ArrayList<>(unmappedFields), er.output()))
            );
    }
}
