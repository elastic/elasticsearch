/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Avg;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsStarTreeQueryExec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Helper class to check if an aggregation query is eligible for star-tree acceleration.
 * <p>
 * A query is eligible if:
 * <ul>
 *     <li>All GROUP BY fields are star-tree grouping fields</li>
 *     <li>All aggregated fields are star-tree value fields</li>
 *     <li>All aggregation types are supported (SUM, COUNT, MIN, MAX, AVG)</li>
 *     <li>Any WHERE filters are on star-tree grouping fields</li>
 * </ul>
 */
public final class StarTreeEligibilityChecker {

    private static final Logger logger = LogManager.getLogger(StarTreeEligibilityChecker.class);

    private StarTreeEligibilityChecker() {}

    /**
     * Configuration for a star-tree grouping field.
     */
    public record GroupingFieldConfig(String field, boolean isDateHistogram, long intervalMillis) {
        /**
         * Creates a regular (ordinal) grouping field config.
         */
        public static GroupingFieldConfig ordinal(String field) {
            return new GroupingFieldConfig(field, false, -1);
        }

        /**
         * Creates a date_histogram grouping field config.
         */
        public static GroupingFieldConfig dateHistogram(String field, long intervalMillis) {
            return new GroupingFieldConfig(field, true, intervalMillis);
        }
    }

    /**
     * Result of eligibility check.
     */
    public record EligibilityResult(
        boolean eligible,
        String starTreeName,
        List<String> groupByFields,
        List<EsStarTreeQueryExec.GroupingFieldFilter> groupingFieldFilters,
        List<EsStarTreeQueryExec.StarTreeAgg> aggregations,
        List<Attribute> outputAttributes
    ) {
        public static EligibilityResult notEligible() {
            return new EligibilityResult(false, null, List.of(), List.of(), List.of(), List.of());
        }
    }

    /**
     * Check if an aggregation is eligible for star-tree acceleration.
     *
     * @param aggregate the aggregate execution plan
     * @param starTreeGroupingFields available star-tree grouping fields
     * @param starTreeValues available star-tree value fields
     * @param starTreeName name of the star-tree
     * @param context optimizer context
     * @return eligibility result
     */
    public static EligibilityResult checkEligibility(
        AggregateExec aggregate,
        Set<String> starTreeGroupingFields,
        Set<String> starTreeValues,
        String starTreeName,
        LocalPhysicalOptimizerContext context
    ) {
        return checkEligibility(aggregate, starTreeGroupingFields, starTreeValues, starTreeName, context, aggregate.getMode());
    }

    /**
     * Check if an aggregation is eligible for star-tree acceleration with a specific aggregator mode.
     *
     * @param aggregate the aggregate execution plan
     * @param starTreeGroupingFields available star-tree grouping fields
     * @param starTreeValues available star-tree value fields
     * @param starTreeName name of the star-tree
     * @param context optimizer context
     * @param mode the aggregator mode (SINGLE or INITIAL)
     * @return eligibility result
     */
    public static EligibilityResult checkEligibility(
        AggregateExec aggregate,
        Set<String> starTreeGroupingFields,
        Set<String> starTreeValues,
        String starTreeName,
        LocalPhysicalOptimizerContext context,
        AggregatorMode mode
    ) {
        // Convert Set<String> to Map<String, GroupingFieldConfig> for backwards compatibility
        // (assumes all fields are ordinal when no config is provided)
        Map<String, GroupingFieldConfig> groupingFieldConfigs = new java.util.HashMap<>();
        for (String field : starTreeGroupingFields) {
            groupingFieldConfigs.put(field, GroupingFieldConfig.ordinal(field));
        }
        return checkEligibility(aggregate, groupingFieldConfigs, starTreeValues, starTreeName, context, mode);
    }

    /**
     * Check if an aggregation is eligible for star-tree acceleration with full grouping field configuration.
     *
     * @param aggregate the aggregate execution plan
     * @param starTreeGroupingFieldConfigs map of field name to grouping field configuration
     * @param starTreeValues available star-tree value fields
     * @param starTreeName name of the star-tree
     * @param context optimizer context
     * @param mode the aggregator mode (SINGLE or INITIAL)
     * @return eligibility result
     */
    public static EligibilityResult checkEligibility(
        AggregateExec aggregate,
        Map<String, GroupingFieldConfig> starTreeGroupingFieldConfigs,
        Set<String> starTreeValues,
        String starTreeName,
        LocalPhysicalOptimizerContext context,
        AggregatorMode mode
    ) {
        return checkEligibility(aggregate, starTreeGroupingFieldConfigs, starTreeValues, starTreeName, context, mode, java.util.Collections.emptyMap());
    }

    /**
     * Check if an aggregation is eligible for star-tree acceleration with full grouping field configuration
     * and field mappings from EvalExec (for bucket() support).
     *
     * @param aggregate the aggregate execution plan
     * @param starTreeGroupingFieldConfigs map of field name to grouping field configuration
     * @param starTreeValues available star-tree value fields
     * @param starTreeName name of the star-tree
     * @param context optimizer context
     * @param mode the aggregator mode (SINGLE or INITIAL)
     * @param evalFieldMappingsInfo map from alias name to field mapping info (from EvalExec)
     * @return eligibility result
     */
    public static EligibilityResult checkEligibility(
        AggregateExec aggregate,
        Map<String, GroupingFieldConfig> starTreeGroupingFieldConfigs,
        Set<String> starTreeValues,
        String starTreeName,
        LocalPhysicalOptimizerContext context,
        AggregatorMode mode,
        Map<String, PushStatsToStarTree.EvalFieldMappingInfo> evalFieldMappingsInfo
    ) {
        // Check group by fields
        List<String> groupByFields = new ArrayList<>();
        // foldCtx is only needed for Bucket functions - get it lazily only when needed
        FoldContext foldCtx = context != null ? context.foldCtx() : null;
        logger.debug("Checking eligibility: {} groupings, starTreeGroupingFieldConfigs={}", aggregate.groupings().size(), starTreeGroupingFieldConfigs.keySet());
        for (Expression grouping : aggregate.groupings()) {
            logger.debug("Processing grouping expression: {} (type: {})", grouping, grouping.getClass().getSimpleName());
            // Unwrap Alias if present (e.g., "BY ts = bucket(@timestamp, 1 hour)")
            Expression actualGrouping = grouping;
            if (grouping instanceof Alias alias) {
                actualGrouping = alias.child();
                logger.debug("Unwrapped alias, actual grouping: {} (type: {})", actualGrouping, actualGrouping.getClass().getSimpleName());
            }

            if (actualGrouping instanceof FieldAttribute fa) {
                String fieldName = fa.fieldName().string();
                if (starTreeGroupingFieldConfigs.containsKey(fieldName) == false) {
                    // GROUP BY field is not a star-tree grouping field
                    return EligibilityResult.notEligible();
                }
                groupByFields.add(fieldName);
            } else if (actualGrouping instanceof ReferenceAttribute ra) {
                // ReferenceAttribute - look up the underlying field from eval mappings
                String refName = ra.name();
                logger.debug("Found ReferenceAttribute: {}", refName);
                PushStatsToStarTree.EvalFieldMappingInfo mappingInfo = evalFieldMappingsInfo.get(refName);
                if (mappingInfo == null) {
                    logger.debug("No eval mapping found for reference: {}", refName);
                    return EligibilityResult.notEligible();
                }
                String underlyingField = mappingInfo.fieldName();
                logger.debug("Found underlying field for reference: {} -> {}", refName, underlyingField);
                GroupingFieldConfig config = starTreeGroupingFieldConfigs.get(underlyingField);
                if (config == null) {
                    logger.debug("Field [{}] is not a star-tree grouping field", underlyingField);
                    return EligibilityResult.notEligible();
                }
                // Check interval compatibility for date_histogram fields
                if (config.isDateHistogram()) {
                    long queryIntervalMillis = mappingInfo.queryIntervalMillis();
                    if (queryIntervalMillis <= 0) {
                        // No interval info - can't determine compatibility
                        logger.debug("No interval info for ReferenceAttribute {}, cannot check compatibility", refName);
                        return EligibilityResult.notEligible();
                    }
                    if (queryIntervalMillis < config.intervalMillis()) {
                        // Query bucket interval is finer than star-tree interval - cannot use star-tree
                        logger.debug(
                            "Query interval {}ms is finer than star-tree interval {}ms for field {}",
                            queryIntervalMillis,
                            config.intervalMillis(),
                            underlyingField
                        );
                        return EligibilityResult.notEligible();
                    }
                    // Check if query interval is an exact multiple of star-tree interval
                    if (queryIntervalMillis % config.intervalMillis() != 0) {
                        logger.debug(
                            "Query interval {}ms is not a multiple of star-tree interval {}ms for field {}",
                            queryIntervalMillis,
                            config.intervalMillis(),
                            underlyingField
                        );
                        return EligibilityResult.notEligible();
                    }
                    logger.debug(
                        "Query interval {}ms is compatible with star-tree interval {}ms for field {}",
                        queryIntervalMillis,
                        config.intervalMillis(),
                        underlyingField
                    );
                }
                groupByFields.add(underlyingField);
            } else if (actualGrouping instanceof Bucket bucket) {
                // Bucket function - check if the underlying field is a star-tree date_histogram grouping field
                logger.debug("Found Bucket function in grouping");
                Expression bucketField = bucket.field();
                logger.debug("Bucket field: {} (type: {})", bucketField, bucketField.getClass().getSimpleName());
                if (bucketField instanceof FieldAttribute == false) {
                    // Bucket over a complex expression - not supported
                    logger.debug("Bucket field is not a FieldAttribute, returning not eligible");
                    return EligibilityResult.notEligible();
                }
                FieldAttribute bucketFieldAttr = (FieldAttribute) bucketField;
                String fieldName = bucketFieldAttr.fieldName().string();
                logger.debug("Bucket field name: {}", fieldName);
                GroupingFieldConfig config = starTreeGroupingFieldConfigs.get(fieldName);
                if (config == null) {
                    // Field is not a star-tree grouping field
                    logger.debug("Field [{}] is not in star-tree grouping fields", fieldName);
                    return EligibilityResult.notEligible();
                }
                if (config.isDateHistogram() == false) {
                    // Field is not a date_histogram grouping field - bucket() requires date_histogram
                    logger.debug("Field [{}] is not a date_histogram field", fieldName);
                    return EligibilityResult.notEligible();
                }
                // Check if bucket interval is compatible with star-tree interval
                // The query bucket interval must be >= star-tree interval (coarser or equal granularity)
                if (foldCtx == null) {
                    // Cannot check bucket interval without fold context
                    return EligibilityResult.notEligible();
                }
                Rounding.Prepared dateRounding = bucket.getDateRoundingOrNull(foldCtx);
                if (dateRounding == null) {
                    // Numeric bucket - not supported for date_histogram fields
                    return EligibilityResult.notEligible();
                }
                // Get the bucket interval in milliseconds
                // For time-based rounding, we estimate the interval by looking at a sample timestamp
                long sampleTimestamp = 1704067200000L; // 2024-01-01T00:00:00Z
                long roundedStart = dateRounding.round(sampleTimestamp);
                long roundedEnd = dateRounding.nextRoundingValue(roundedStart);
                long queryIntervalMillis = roundedEnd - roundedStart;

                if (queryIntervalMillis < config.intervalMillis()) {
                    // Query bucket interval is finer than star-tree interval - cannot use star-tree
                    return EligibilityResult.notEligible();
                }
                // Query interval is >= star-tree interval, check if it's an exact multiple
                if (queryIntervalMillis % config.intervalMillis() != 0) {
                    // Query interval is not an exact multiple of star-tree interval - cannot use star-tree
                    // (e.g., star-tree has 1h interval, query has 90m interval)
                    return EligibilityResult.notEligible();
                }
                groupByFields.add(fieldName);
            } else {
                // Non-field grouping (e.g., expression) - not supported
                return EligibilityResult.notEligible();
            }
        }

        // Check aggregations
        List<EsStarTreeQueryExec.StarTreeAgg> aggregations = new ArrayList<>();
        List<Attribute> outputAttributes = new ArrayList<>();

        for (NamedExpression agg : aggregate.aggregates()) {
            if (agg instanceof Alias alias) {
                Expression child = alias.child();
                // Check if this is a bucket function (passthrough grouping field)
                if (child instanceof Bucket bucket) {
                    Expression bucketField = bucket.field();
                    if (bucketField instanceof FieldAttribute fa) {
                        String fieldName = fa.fieldName().string();
                        if (groupByFields.contains(fieldName)) {
                            outputAttributes.add(agg.toAttribute());
                            continue;
                        }
                    }
                    return EligibilityResult.notEligible();
                }
                EsStarTreeQueryExec.StarTreeAgg starTreeAgg = extractStarTreeAgg(child, starTreeValues, mode);
                if (starTreeAgg == null) {
                    return EligibilityResult.notEligible();
                }
                aggregations.add(new EsStarTreeQueryExec.StarTreeAgg(
                    starTreeAgg.valueField(),
                    starTreeAgg.type(),
                    alias.name(),
                    starTreeAgg.isLongField()
                ));
                outputAttributes.add(agg.toAttribute());
            } else if (agg instanceof FieldAttribute fa) {
                // Pass-through grouping field in output
                if (groupByFields.contains(fa.fieldName().string())) {
                    outputAttributes.add(agg.toAttribute());
                } else {
                    return EligibilityResult.notEligible();
                }
            } else if (agg instanceof ReferenceAttribute ra) {
                // ReferenceAttribute - check if it's a pass-through grouping field via eval mappings
                String refName = ra.name();
                PushStatsToStarTree.EvalFieldMappingInfo mappingInfo = evalFieldMappingsInfo.get(refName);
                String underlyingField = mappingInfo != null ? mappingInfo.fieldName() : null;
                logger.debug("Checking ReferenceAttribute in aggregates: {} -> underlying: {}", refName, underlyingField);
                if (underlyingField != null && groupByFields.contains(underlyingField)) {
                    outputAttributes.add(agg.toAttribute());
                } else {
                    logger.debug("ReferenceAttribute {} not in groupByFields: {}", refName, groupByFields);
                    return EligibilityResult.notEligible();
                }
            } else {
                logger.debug("Unknown aggregate type: {} ({})", agg, agg.getClass().getSimpleName());
                return EligibilityResult.notEligible();
            }
        }

        if (aggregations.isEmpty()) {
            return EligibilityResult.notEligible();
        }

        return new EligibilityResult(
            true,
            starTreeName,
            groupByFields,
            List.of(), // Grouping field filters will be extracted from the query
            aggregations,
            outputAttributes
        );
    }

    /**
     * Extract star-tree aggregation from an expression.
     *
     * @param expr the aggregation expression
     * @param starTreeValues available star-tree value fields
     * @param mode the aggregator mode
     * @return the star-tree aggregation, or null if not supported
     */
    private static EsStarTreeQueryExec.StarTreeAgg extractStarTreeAgg(Expression expr, Set<String> starTreeValues, AggregatorMode mode) {
        if (expr instanceof Sum sum) {
            return extractValueAgg(sum.field(), starTreeValues, EsStarTreeQueryExec.StarTreeAggType.SUM);
        } else if (expr instanceof Count count) {
            Expression field = count.field();
            if (field.foldable()) {
                // COUNT(*) - use any value field, COUNT always produces long values
                if (starTreeValues.isEmpty() == false) {
                    return new EsStarTreeQueryExec.StarTreeAgg(
                        starTreeValues.iterator().next(),
                        EsStarTreeQueryExec.StarTreeAggType.COUNT,
                        null,
                        true  // COUNT always produces long
                    );
                }
            }
            return extractValueAgg(field, starTreeValues, EsStarTreeQueryExec.StarTreeAggType.COUNT);
        } else if (expr instanceof Min min) {
            return extractValueAgg(min.field(), starTreeValues, EsStarTreeQueryExec.StarTreeAggType.MIN);
        } else if (expr instanceof Max max) {
            return extractValueAgg(max.field(), starTreeValues, EsStarTreeQueryExec.StarTreeAggType.MAX);
        } else if (expr instanceof Avg avg) {
            // AVG is only supported in SINGLE mode because INITIAL mode requires emitting SUM+COUNT
            // separately, which would require restructuring the aggregation
            if (mode == AggregatorMode.SINGLE) {
                return extractValueAgg(avg.field(), starTreeValues, EsStarTreeQueryExec.StarTreeAggType.AVG);
            }
            // In INITIAL mode, AVG needs to emit partial state (sum, count) which requires
            // different handling. For now, we don't support AVG in INITIAL mode.
            return null;
        }
        return null;
    }

    /**
     * Extract value aggregation for a field expression.
     */
    private static EsStarTreeQueryExec.StarTreeAgg extractValueAgg(
        Expression field,
        Set<String> starTreeValues,
        EsStarTreeQueryExec.StarTreeAggType type
    ) {
        if (field instanceof FieldAttribute fa) {
            String fieldName = fa.fieldName().string();
            if (starTreeValues.contains(fieldName)) {
                boolean isLongField = isIntegerType(fa.dataType());
                return new EsStarTreeQueryExec.StarTreeAgg(fieldName, type, null, isLongField);
            }
        }
        return null;
    }

    /**
     * Check if a data type is an integer type (long, int, short, byte).
     */
    private static boolean isIntegerType(DataType dataType) {
        return dataType == DataType.LONG
            || dataType == DataType.INTEGER
            || dataType == DataType.SHORT
            || dataType == DataType.BYTE
            || dataType == DataType.UNSIGNED_LONG;
    }
}
