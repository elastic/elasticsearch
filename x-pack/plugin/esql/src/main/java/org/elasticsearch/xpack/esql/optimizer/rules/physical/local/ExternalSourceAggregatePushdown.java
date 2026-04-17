/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.datasources.FileSplit;
import org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Shared helpers for aggregate pushdown rules ({@link PushStatsToExternalSource} and
 * {@link PushAggregatesToExternalSource}) that extract an {@link ExternalSourceExec}
 * from the plan tree and resolve filtered metadata using {@link SplitFilterClassifier}.
 */
final class ExternalSourceAggregatePushdown {

    private ExternalSourceAggregatePushdown() {}

    /**
     * Parsed result from the subtree below an {@code AggregateExec}: the external source,
     * any alias mapping from intermediate {@code EvalExec}/{@code ProjectExec} nodes, and
     * the filter condition from any intermediate {@code FilterExec}.
     */
    record ExternalSourceInfo(ExternalSourceExec externalExec, AttributeMap<Attribute> aliasReplacedBy, Expression filterCondition) {}

    /**
     * Extracts the ExternalSourceExec and optional filter/alias information from the plan
     * subtree below an AggregateExec. Supports these patterns:
     * <ul>
     *   <li>{@code ExternalSourceExec}</li>
     *   <li>{@code EvalExec -> ExternalSourceExec}</li>
     *   <li>{@code ProjectExec -> ExternalSourceExec}</li>
     *   <li>{@code FilterExec -> ExternalSourceExec}</li>
     *   <li>{@code FilterExec -> EvalExec -> ExternalSourceExec}</li>
     *   <li>{@code FilterExec -> ProjectExec -> ExternalSourceExec}</li>
     * </ul>
     * Returns null if the subtree doesn't match any recognized pattern.
     */
    static ExternalSourceInfo extractExternalSource(PhysicalPlan child) {
        if (child instanceof ExternalSourceExec ext) {
            return new ExternalSourceInfo(ext, AttributeMap.emptyAttributeMap(), null);
        }
        if (child instanceof EvalExec evalExec && evalExec.child() instanceof ExternalSourceExec ext) {
            return new ExternalSourceInfo(ext, PushFiltersToSource.getAliasReplacedBy(evalExec), null);
        }
        if (child instanceof ProjectExec projectExec && projectExec.child() instanceof ExternalSourceExec ext) {
            return new ExternalSourceInfo(ext, PushFiltersToSource.getAliasReplacedBy(projectExec), null);
        }
        if (child instanceof FilterExec filterExec) {
            PhysicalPlan filterChild = filterExec.child();
            if (filterChild instanceof ExternalSourceExec ext) {
                return new ExternalSourceInfo(ext, AttributeMap.emptyAttributeMap(), filterExec.condition());
            }
            if (filterChild instanceof EvalExec evalExec && evalExec.child() instanceof ExternalSourceExec ext) {
                return new ExternalSourceInfo(ext, PushFiltersToSource.getAliasReplacedBy(evalExec), filterExec.condition());
            }
            if (filterChild instanceof ProjectExec projectExec && projectExec.child() instanceof ExternalSourceExec ext) {
                return new ExternalSourceInfo(ext, PushFiltersToSource.getAliasReplacedBy(projectExec), filterExec.condition());
            }
        }
        return null;
    }

    /**
     * Resolves effective metadata for splits filtered by the given condition. Evaluates
     * the filter against per-split statistics, classifying each split as MATCH, MISS, or
     * AMBIGUOUS. Returns merged statistics from MATCH-only splits, or null if any split
     * is AMBIGUOUS or classification fails.
     * <p>
     * When a single split is present and has its own statistics, those are preferred over
     * file-level metadata to avoid misclassification when split stats differ from the whole.
     */
    static Map<String, Object> resolveFilteredMetadata(ExternalSourceExec externalExec, Expression filterCondition) {
        List<? extends ExternalSplit> splits = externalExec.splits();

        if (splits.isEmpty() || splits.size() == 1) {
            Map<String, Object> metadata = null;
            if (splits.size() == 1
                && splits.getFirst() instanceof FileSplit fs
                && fs.statistics() != null
                && fs.statistics().isEmpty() == false) {
                metadata = fs.statistics();
            }
            if (metadata == null) {
                metadata = externalExec.sourceMetadata();
            }
            if (metadata == null || metadata.isEmpty()) {
                return null;
            }
            SplitFilterClassifier.SplitMatch result = SplitFilterClassifier.classifyExpression(filterCondition, metadata);
            return switch (result) {
                case MATCH -> metadata;
                case MISS -> emptyStatsMetadata();
                case AMBIGUOUS -> null;
            };
        }

        List<Map<String, Object>> matchedStats = new ArrayList<>();
        for (ExternalSplit split : splits) {
            if (split instanceof FileSplit == false) {
                return null;
            }
            Map<String, Object> stats = ((FileSplit) split).statistics();
            if (stats == null || stats.isEmpty()) {
                return null;
            }
            SplitFilterClassifier.SplitMatch result = SplitFilterClassifier.classifyExpression(filterCondition, stats);
            switch (result) {
                case MATCH -> matchedStats.add(stats);
                case MISS -> {
                }
                case AMBIGUOUS -> {
                    return null;
                }
            }
        }

        if (matchedStats.isEmpty()) {
            return emptyStatsMetadata();
        }
        return SourceStatisticsSerializer.mergeStatistics(matchedStats);
    }

    static Map<String, Object> emptyStatsMetadata() {
        return Map.of(SourceStatisticsSerializer.STATS_ROW_COUNT, 0L);
    }
}
