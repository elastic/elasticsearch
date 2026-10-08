/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.local;

import org.elasticsearch.index.mapper.KeywordFieldMapper.KeywordFieldType;
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
 * On a given data node, however, the field may actually be mapped on every local shard, so we replace the marker with a regular
 * {@link KeywordEsField} when that is the case.
 */
public class ReplacePotentiallyUnmappedFieldWithMappedField extends ParameterizedRule<
    LogicalPlan,
    LogicalPlan,
    LocalLogicalOptimizerContext> {

    @Override
    public LogicalPlan apply(LogicalPlan plan, LocalLogicalOptimizerContext localLogicalOptimizerContext) {
        SearchStats searchStats = localLogicalOptimizerContext.searchStats();
        return plan.transformExpressionsDown(FieldAttribute.class, fieldAttribute -> {
            if (fieldAttribute.field() instanceof PotentiallyUnmappedKeywordEsField potentiallyUnmapped) {
                var fieldName = fieldAttribute.fieldName();
                boolean hasDocValues = searchStats.hasDocValues(fieldName);
                // isIndexed and hasDocValues are AND-ed across shards (see SearchContextStats), so either being true proves the field is
                // mapped everywhere here. We deliberately do not use exists(), which is OR-ed across shards.
                if (searchStats.isIndexed(fieldName) || hasDocValues) {
                    // The marker's normalized flag is always false; read the real value from the mapped type so that exact-match
                    // pushdown, which is unsafe on a normalized keyword, stays disabled when a normalizer is present.
                    boolean normalized = searchStats.fieldType(fieldName) instanceof KeywordFieldType keywordFieldType
                        ? keywordFieldType.hasNormalizer()
                        : potentiallyUnmapped.getNormalized();
                    return fieldAttribute.withField(
                        new KeywordEsField(
                            potentiallyUnmapped.getName(),
                            potentiallyUnmapped.getProperties(),
                            hasDocValues,
                            potentiallyUnmapped.getPrecision(),
                            normalized,
                            potentiallyUnmapped.isAlias(),
                            potentiallyUnmapped.getTimeSeriesFieldType()
                        )
                    );
                }
            }
            return fieldAttribute;
        });
    }
}
