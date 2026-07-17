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
 * A {@link PotentiallyUnmappedKeywordEsField} marks a keyword that may be unmapped on some shards (e.g. because it is mapped in some
 * indices of a pattern but not others, or because {@code unmapped_fields="load"} loads a field that isn't in the mapping). It is loaded
 * from {@code _source} and, crucially, is never pushed down to Lucene: pushing a predicate to a shard where the field is unmapped would
 * silently drop rows.
 * <p>
 * That marker is conservative and coordinator-wide. On a given data node, however, the field may actually be mapped on <em>every</em>
 * shard we hold here (this is exactly the case when a field mapped in only some indices lands on a node holding only those indices). When
 * {@link SearchStats#isIndexed} confirms that, the field is a plain keyword as far as this node is concerned, so we replace the marker with
 * a regular {@link KeywordEsField}. The attribute id is preserved, so all downstream references stay valid; the replacement simply unblocks
 * the usual keyword optimizations -- most notably filter and TopN pushdown to Lucene.
 * <p>
 * Only the pure-keyword {@link PotentiallyUnmappedKeywordEsField} is handled here. The type-conflict variant
 * {@code PotentiallyUnmappedSingleTypeEsField} is deliberately left alone: it still needs cast/fallback resolution and is not a plain
 * keyword on any shard.
 */
public class ReplacePotentiallyUnmappedFieldWithMappedField extends ParameterizedRule<
    LogicalPlan,
    LogicalPlan,
    LocalLogicalOptimizerContext> {

    @Override
    public LogicalPlan apply(LogicalPlan plan, LocalLogicalOptimizerContext localLogicalOptimizerContext) {
        SearchStats searchStats = localLogicalOptimizerContext.searchStats();
        return plan.transformExpressionsDown(FieldAttribute.class, fieldAttribute -> {
            // Safe only because isIndexed means the field is mapped and indexed on every local shard: no shard here needs the _source
            // fallback that the potentially-unmapped marker exists to trigger.
            if (fieldAttribute.field() instanceof PotentiallyUnmappedKeywordEsField potentiallyUnmapped
                && searchStats.isIndexed(fieldAttribute.fieldName())) {
                boolean hasDocValues = searchStats.hasDocValues(fieldAttribute.fieldName());
                return fieldAttribute.withField(asMappedKeyword(potentiallyUnmapped, hasDocValues));
            }
            return fieldAttribute;
        });
    }

    private static KeywordEsField asMappedKeyword(PotentiallyUnmappedKeywordEsField potentiallyUnmapped, boolean hasDocValues) {
        // Reproduce what IndexResolver builds for a mapped keyword: precision Short.MAX_VALUE and normalized=false (ES|QL does not track
        // keyword normalizers today), keeping the multi-field properties. Doc values come from SearchStats rather than the marker's
        // optimistic default, so this node's real mapping decides whether doc-value-dependent pushdowns (TopN, single-value filters) apply.
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
