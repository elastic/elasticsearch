/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedKeywordEsField;
import org.elasticsearch.xpack.esql.expression.NamedExpressions;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Merges unmapped fields into the output of the ES relation. This marking is necessary for the block loaders to force loading from _source
 * if the field is unmapped.
 *
 * N.B. This is only used for INSIST keyword, so when INSIST is sunset, we can get rid of this rule!
 */
public class PropagateUnmappedFields extends Rule<LogicalPlan, LogicalPlan> {
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
        return unmappedFields.isEmpty() ? logicalPlan : logicalPlan.transformUp(EsRelation.class, er -> mergeMissing(er, unmappedFields));
    }

    private static EsRelation mergeMissing(EsRelation er, AttributeSet unmappedFields) {
        Set<String> existingPuks = new HashSet<>();
        for (Attribute attr : er.output()) {
            if (attr instanceof FieldAttribute fa && fa.field() instanceof PotentiallyUnmappedKeywordEsField) {
                existingPuks.add(fa.fieldName().string());
            }
        }
        // Partially-mapped keyword fields are already in the EsRelation output as
        // PUKs (via IndexResolver.wrapPartiallyUnmappedField); this rule only adds
        // PUKs introduced by INSIST on a field that is not in the index.
        List<Attribute> missing = new ArrayList<>();
        for (Attribute attr : unmappedFields) {
            if (attr instanceof FieldAttribute fa && existingPuks.contains(fa.fieldName().string()) == false) {
                missing.add(attr);
            }
        }
        return missing.isEmpty() ? er : er.withAttributes(NamedExpressions.mergeOutputAttributes(missing, er.output()));
    }
}
