/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.local;

import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.type.KeywordEsField;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedKeywordEsField;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;
import org.elasticsearch.xpack.esql.stats.SearchStats;

/**
 * A {@link PotentiallyUnmappedKeywordEsField} marks a keyword that may be unmapped on some shards (e.g., because it is mapped in some
 * indices of a pattern but not others, or because {@code unmapped_fields="load"} loads a field that isn't in the mapping). It is loaded
 * from {@code _source} and, crucially, is never pushed down to Lucene.
 * <p>
 * On a given data node, however, the field may actually be mapped, so we replace the marker with a regular {@link KeywordEsField} when that
 * is the case.
 */
public class ReplacePotentiallyUnmappedFieldWithMappedField extends ParameterizedRule<
    LogicalPlan,
    LogicalPlan,
    LocalLogicalOptimizerContext> {

    @Override
    public LogicalPlan apply(LogicalPlan plan, LocalLogicalOptimizerContext localLogicalOptimizerContext) {
        SearchStats searchStats = localLogicalOptimizerContext.searchStats();
        return plan.transformExpressionsDown(FieldAttribute.class, fieldAttribute -> {
            if (fieldAttribute.field() instanceof PotentiallyUnmappedKeywordEsField potentiallyUnmapped
                && searchStats.isIndexed(fieldAttribute.fieldName())) {
                boolean hasDocValues = searchStats.hasDocValues(fieldAttribute.fieldName());
                return fieldAttribute.withField(asMappedKeyword(potentiallyUnmapped, hasDocValues));
            }
            return fieldAttribute;
        });
    }

    private static KeywordEsField asMappedKeyword(PotentiallyUnmappedKeywordEsField potentiallyUnmapped, boolean hasDocValues) {
        return new KeywordEsField(
            potentiallyUnmapped.getName(),
            potentiallyUnmapped.getProperties(),
            hasDocValues,
            potentiallyUnmapped.getPrecision(),
            potentiallyUnmapped.getNormalized(),
            potentiallyUnmapped.isAlias(),
            potentiallyUnmapped.getTimeSeriesFieldType()
        );
    }
}
