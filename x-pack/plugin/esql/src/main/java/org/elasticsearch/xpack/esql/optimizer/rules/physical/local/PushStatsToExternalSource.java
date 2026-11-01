/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.common.Strings;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.FormatReaderRegistry;
import org.elasticsearch.xpack.esql.datasources.pushdown.PushdownLiteralConversion;
import org.elasticsearch.xpack.esql.datasources.pushdown.PushdownPredicates;
import org.elasticsearch.xpack.esql.datasources.spi.AggregatePushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.FilterPushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Pushes ungrouped aggregate functions (COUNT, MIN, MAX) to external sources when the
 * required statistics are available in the file metadata. Replaces the AggregateExec +
 * ExternalSourceExec subtree with a LocalSourceExec containing pre-computed results.
 * <p>
 * Supports both SINGLE and INITIAL modes. In SINGLE mode the replacement produces final-value
 * blocks (one block per aggregate). In INITIAL mode the replacement produces intermediate-format
 * blocks matching {@link AggregateExec#intermediateAttributes()}: for each aggregate, a typed
 * value block followed by a {@code seen} boolean block.
 * <p>
 * Handles intermediate EvalExec (from EVAL), ProjectExec (from RENAME), and FilterExec
 * nodes between the aggregate and external source by resolving aliased attribute names
 * back to the original column names before metadata lookup, and by classifying filters
 * against per-split statistics when present.
 * <p>
 * Supports multi-split queries when per-split statistics are available in
 * {@link org.elasticsearch.xpack.esql.datasources.FileSplit#splitStats()}.
 * Statistics are merged across splits (sum row counts, min-of-mins, max-of-maxes).
 * Falls back to normal execution when any split lacks stats.
 * <p>
 * Substitution from metadata statistics is skipped when scan-only predicates remain after
 * {@link PushFiltersToSource}. The gate is {@code filterCondition == null} (no remaining
 * {@code FilterExec} to classify — typical of a pure {@link FilterPushdownSupport.Pushability#YES}
 * push), any pushed expression that is not explicitly
 * {@link FilterPushdownSupport.Pushability#RECHECK} (YES conjuncts are dropped from the remainder;
 * {@link FilterPushdownSupport.Pushability#NO} / an un-overridden SPI default gives no coverage
 * guarantee), or any pushed expression absent from {@code rewrite(filterCondition)}. When every
 * pushed expression is RECHECK, covered by the rewritten remainder, and a {@code FilterExec}
 * remains, this rule classifies that filter against split stats — including matching-type
 * comparisons and converted mixed date/numeric leaves (pushed rewritten, remainder original).
 * <p>
 * Note: MIN/MAX pushdown uses values from file metadata. Temporal columns (DATE/TIMESTAMP/INT96)
 * are decoded to ESQL's epoch-millisecond representation at stat-publication time by the format
 * reader (see {@code ParquetColumnDecoding#decodeTemporalStat}), so the pushed values already match
 * the scan path.
 */
public class PushStatsToExternalSource extends PhysicalOptimizerRules.ParameterizedOptimizerRule<
    AggregateExec,
    LocalPhysicalOptimizerContext> {

    private static final Logger logger = LogManager.getLogger(PushStatsToExternalSource.class);

    @Override
    protected PhysicalPlan rule(AggregateExec aggregateExec, LocalPhysicalOptimizerContext ctx) {
        ExternalSourceAggregatePushdown.ExternalSourceInfo info = ExternalSourceAggregatePushdown.extractExternalSource(
            aggregateExec.child()
        );
        if (info == null) {
            return aggregateExec;
        }
        ExternalSourceExec externalExec = info.externalExec();
        AttributeMap<Attribute> aliasReplacedBy = info.aliasReplacedBy();
        Expression filterCondition = info.filterCondition();

        // Consulting the format's implicit-nulls declaration requires the registry. Honor the
        // ExternalOptimizerContext.NONE contract — treat a missing registry as "no information" and bail.
        // (A NONE context never carries an ExternalSourceExec today, but bailing keeps the rule and the
        // documented contract in lockstep.)
        FormatReaderRegistry formatReaderRegistry = ctx == null || ctx.external() == null ? null : ctx.external().formatReaderRegistry();
        if (formatReaderRegistry == null) {
            return aggregateExec;
        }
        FormatReader formatReader = formatReaderRegistry.findByName(externalExec.sourceType());
        if (formatReader == null || formatReader.aggregatePushdownSupport() == AggregatePushdownSupport.UNSUPPORTED) {
            return aggregateExec;
        }
        boolean implicitNullsForAbsentColumn = formatReader.aggregatePushdownSupport().appliesImplicitNullsForAbsentColumn();

        AggregatorMode mode = aggregateExec.getMode();
        if (mode != AggregatorMode.SINGLE && mode != AggregatorMode.INITIAL) {
            return aggregateExec;
        }

        if (aggregateExec.groupings().isEmpty() == false) {
            return aggregateExec;
        }

        // Row counts describe whole splits before scan-time predicates. Bail unless every pushed
        // expression is RECHECK and a FilterExec remains to classify; otherwise fold would ignore
        // scan-only conjuncts.
        if (hasScanOnlyPushedPredicates(externalExec, filterCondition, formatReader)) {
            logger.info(
                () -> Strings.format(
                    "PushStatsToExternalSource: skipping stats substitution (source has scan-only pushed predicates)"
                        + " path=[{}] projections=[{}] type=[{}]",
                    externalExec.sourcePath(),
                    externalExec.pushedExpressions().size(),
                    externalExec.sourceType()
                )
            );
            return aggregateExec;
        }

        Expression filterForClassification = filterCondition;
        if (filterCondition != null && aliasReplacedBy.isEmpty() == false) {
            filterForClassification = filterCondition.transformDown(ReferenceAttribute.class, r -> aliasReplacedBy.resolve(r, r));
        }

        // Computed aliases have no footer statistics, even when they share a name with a source column.
        // After resolving identity aliases, require every reference to belong to the source by attribute id.
        if (filterForClassification != null && externalExec.outputSet().containsAll(filterForClassification.references()) == false) {
            return aggregateExec;
        }

        // SplitFilterClassifier reasons from file-level stats and treats columns physically absent from
        // the file as "all null" (columnNullCount == rowCount). Partition columns live in the directory
        // path, not the payload, so they are absent from every file's stats and would misclassify
        // IS NULL / IS NOT NULL on a partition column as MATCH/MISS for every split. A footer statistic
        // also never describes an engine-materialised column (MetadataAttribute or VirtualAttribute),
        // so those conjuncts take the same bail. The scan path evaluates them against the
        // VirtualColumnIterator's constant or per-row block.
        // Read the partition set from serialized sourceMetadata (not the coordinator-only fileList): on a data node
        // externalExec.fileList() is UNRESOLVED, so COUNT(partition_col) would otherwise fold to 0 there.
        Set<String> pathDerivedColumns = externalExec.partitionColumnNames();
        if (filterForClassification != null
            && (referencesAnyColumn(filterForClassification, pathDerivedColumns) || referencesVirtualColumn(filterForClassification))) {
            return aggregateExec;
        }

        org.elasticsearch.xpack.esql.datasources.spi.SplitStats stats;
        if (filterForClassification != null) {
            stats = ExternalSourceAggregatePushdown.resolveFilteredStats(
                externalExec,
                filterForClassification,
                implicitNullsForAbsentColumn
            );
        } else {
            stats = externalExec.effectiveSplitStats();
        }
        if (stats == null) {
            return aggregateExec;
        }
        List<? extends NamedExpression> aggregates = aggregateExec.aggregates();

        // Resolve each aggregate's alias children back to the source columns once (identity for the direct
        // ExternalSourceExec shape), so both the format type-gate below and the value loop see the true columns.
        List<Expression> resolvedAggExprs = new ArrayList<>(aggregates.size());
        for (NamedExpression agg : aggregates) {
            if (agg instanceof Alias == false) {
                return aggregateExec;
            }
            Expression aggExpr = ((Alias) agg).child();
            if (aliasReplacedBy.isEmpty() == false) {
                aggExpr = aggExpr.transformDown(ReferenceAttribute.class, r -> aliasReplacedBy.resolve(r, r));
            }
            resolvedAggExprs.add(aggExpr);
        }

        // Consult the format's declared aggregate pushability before touching stats — each pushdown path (this
        // fold and ComputeService's split-discovery gate) gates on canPushAggregates so the two cannot diverge.
        // Gate on the ALIAS-RESOLVED functions so the type/virtual-column checks see the real columns. No
        // isEmpty() bail: canPushAggregates(List.of(), List.of()) is YES in every impl, preserving today's
        // empty/no-AggregateFunction behavior exactly.
        List<Expression> aggFunctions = ExternalSourceAggregatePushdown.extractAggregateFunctions(resolvedAggExprs);
        if (formatReader.aggregatePushdownSupport()
            .canPushAggregates(aggFunctions, List.of()) != AggregatePushdownSupport.Pushability.YES) {
            return aggregateExec;
        }

        List<Object> values = new ArrayList<>(aggregates.size());
        List<DataType> dataTypes = new ArrayList<>(aggregates.size());

        for (Expression aggExpr : resolvedAggExprs) {
            Object value = ExternalSourceAggregatePushdown.resolveFromStats(
                aggExpr,
                stats,
                implicitNullsForAbsentColumn,
                pathDerivedColumns
            );
            if (value == null) {
                return aggregateExec;
            }
            values.add(value);
            dataTypes.add(aggExpr instanceof AggregateFunction af ? af.dataType() : DataType.LONG);
        }

        List<Attribute> outputAttrs;
        Block[] blocks;
        if (mode == AggregatorMode.SINGLE) {
            outputAttrs = new ArrayList<>(aggregates.size());
            for (NamedExpression agg : aggregates) {
                outputAttrs.add(agg.toAttribute());
            }
            blocks = ExternalSourceAggregatePushdown.buildFinalBlocks(values, dataTypes);
        } else {
            outputAttrs = aggregateExec.intermediateAttributes();
            blocks = ExternalSourceAggregatePushdown.buildIntermediateBlocks(values, dataTypes);
        }

        return new LocalSourceExec(aggregateExec.source(), outputAttrs, LocalSupplier.of(new Page(blocks)));
    }

    /**
     * {@code true} when footer stats must not answer the aggregate because the reader still applies
     * predicates that {@code filterCondition} does not cover.
     * <ul>
     *   <li>No push → {@code false}.</li>
     *   <li>Push with {@code filterCondition == null} (no remaining {@code FilterExec}) → {@code true}.</li>
     *   <li>Opaque {@code pushedFilter} without {@code pushedExpressions} → {@code true}.</li>
     *   <li>Any pushed expression is not {@link FilterPushdownSupport.Pushability#RECHECK} →
     *       {@code true}. Require RECHECK explicitly: YES is dropped from the remainder;
     *       {@link FilterPushdownSupport.Pushability#NO} or an un-overridden {@code canPush} default
     *       gives no guarantee that {@code FilterExec} still holds the conjunct.</li>
     *   <li>Any pushed expression is absent from {@code rewrite(filterCondition)} (after
     *       {@link Predicates#splitAnd}) → {@code true}. Parquet/ORC keep RECHECK remainders as
     *       originals while pushing rewritten forms; rewriting the remainder must recover each
     *       pushed conjunct. Opaque {@code pushedFilter} is not inspected when expressions are
     *       present — it is assumed to be the translation of that list.</li>
     *   <li>Every pushed expression is RECHECK, covered by the rewritten remainder, and
     *       {@code FilterExec} remains → {@code false}.</li>
     * </ul>
     */
    static boolean hasScanOnlyPushedPredicates(ExternalSourceExec externalExec, Expression filterCondition, FormatReader formatReader) {
        boolean hasPushedExpressions = externalExec.pushedExpressions().isEmpty() == false;
        boolean hasPushedFilter = externalExec.pushedFilter() != null;
        if (hasPushedExpressions == false && hasPushedFilter == false) {
            return false;
        }
        if (filterCondition == null) {
            return true;
        }
        if (hasPushedExpressions == false) {
            // Opaque pushedFilter alone: cannot prove RECHECK coverage.
            return true;
        }
        FilterPushdownSupport support = formatReader.filterPushdownSupport();
        if (support == null) {
            return true;
        }
        // Rewrite once: mixed originals in FilterExec become the column-typed forms stored in pushedExpressions.
        List<Expression> coveredByFilter = Predicates.splitAnd(PushdownLiteralConversion.rewrite(filterCondition));
        for (Expression pushed : externalExec.pushedExpressions()) {
            // Require RECHECK explicitly. NO or an un-overridden default canPush gives no guarantee
            // that FilterExec still holds the conjunct.
            if (support.canPush(pushed) != FilterPushdownSupport.Pushability.RECHECK) {
                return true;
            }
            if (coveredByFilter.contains(pushed) == false) {
                return true;
            }
        }
        return false;
    }

    private static boolean referencesAnyColumn(Expression expr, Set<String> columnNames) {
        if (columnNames.isEmpty()) {
            return false;
        }
        return expr.references().stream().anyMatch(a -> columnNames.contains(a.name()));
    }

    private static boolean referencesVirtualColumn(Expression expr) {
        return expr.references().stream().anyMatch(PushdownPredicates::isVirtualColumn);
    }
}
