/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.xpack.esql.querydsl.query.SingleValueQuery;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.RoundTo;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EsStarTreeQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.AbstractPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.expression.Foldables;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Optimizer rule that pushes aggregation queries to star-tree indices when eligible.
 * <p>
 * This rule detects patterns like:
 * <pre>
 *     AggregateExec(groupings, aggregates)
 *         └── EsQueryExec(query)
 * </pre>
 * And transforms them to:
 * <pre>
 *     EsStarTreeQueryExec(starTree, groupingFields, values)
 * </pre>
 * When the aggregation can be satisfied using pre-computed star-tree data.
 */
public class PushStatsToStarTree extends PhysicalOptimizerRules.ParameterizedOptimizerRule<AggregateExec, LocalPhysicalOptimizerContext> {

    private static final Logger logger = LogManager.getLogger(PushStatsToStarTree.class);

    @Override
    protected PhysicalPlan rule(AggregateExec aggregateExec, LocalPhysicalOptimizerContext context) {
        // Star-tree optimization applies to SINGLE mode (non-distributed) and INITIAL mode (data node portion).
        // For INITIAL mode, we output intermediate attributes that match what AggregateExec(FINAL) expects.
        AggregatorMode mode = aggregateExec.getMode();
        if (mode != AggregatorMode.SINGLE && mode != AggregatorMode.INITIAL) {
            logger.debug("Star-tree optimization skipped: aggregator mode is {} (not SINGLE or INITIAL)", mode);
            return aggregateExec;
        }
        logger.debug(
            "Star-tree optimization: checking aggregation with {} groupings and {} aggregates, mode={}",
            aggregateExec.groupings().size(),
            aggregateExec.aggregates().size(),
            mode
        );
        logger.debug("Star-tree: groupings={}", aggregateExec.groupings());

        // Check for pattern: AggregateExec -> [EvalExec ->] [FilterExec ->] EsQueryExec
        // The EvalExec may be present when bucket() function is used (computes ROUNDTO)
        PhysicalPlan child = aggregateExec.child();
        FilterExec filterExec = null;
        EvalExec evalExec = null;
        EsQueryExec queryExec = null;

        logger.debug("Star-tree: child type is {}", child.getClass().getSimpleName());

        // Handle EvalExec (from bucket() function)
        if (child instanceof EvalExec ee) {
            evalExec = ee;
            child = ee.child();
            logger.debug("Star-tree: found EvalExec, next child type is {}", child.getClass().getSimpleName());
        }

        if (child instanceof FilterExec fe) {
            filterExec = fe;
            if (fe.child() instanceof EsQueryExec qe) {
                queryExec = qe;
            }
        } else if (child instanceof EsQueryExec qe) {
            queryExec = qe;
        }

        if (queryExec == null) {
            logger.debug("Star-tree optimization skipped: no EsQueryExec child found, child={}", child);
            return aggregateExec;
        }
        logger.debug("Star-tree optimization: found EsQueryExec for index [{}]", queryExec.indexPattern());

        // Extract field mappings from EvalExec (for bucket() function support)
        // Maps alias name -> field mapping info (field name + bucket interval)
        Map<String, EvalFieldMappingInfo> evalFieldMappingsInfo = extractEvalFieldMappings(evalExec, context);
        if (evalFieldMappingsInfo.isEmpty() == false) {
            logger.debug("Star-tree: found EvalExec field mappings: {}", evalFieldMappingsInfo);
        }

        // Check if there's a query filter that star-tree can't handle.
        // Star-tree works with:
        // - No query (null)
        // - MatchAllQueryBuilder (match all documents)
        // - ExistsQueryBuilder (ES|QL adds this to check field existence, which star-tree handles during pre-aggregation)
        // - TermQueryBuilder on a grouping field (string equality filter like WHERE country == "US")
        // Any other query (e.g., range, bool with filters on non-grouping fields) means there's a WHERE clause
        // that was pushed to the Lucene level, which star-tree can't handle.
        QueryBuilder query = queryExec.query();
        logger.info("Star-tree: query type is [{}], query=[{}]", query == null ? "null" : query.getClass().getSimpleName(), query);

        // Extract grouping field filter from TermQueryBuilder if present
        // This handles WHERE clauses like "country == 'US'" that get pushed to Lucene
        // ES|QL may wrap the term query in a BoolQueryBuilder, so we handle both cases
        EsStarTreeQueryExec.GroupingFieldFilter termQueryFilter = extractTermQueryFilter(query);
        if (termQueryFilter == null && query != null
            && (query instanceof MatchAllQueryBuilder) == false
            && (query instanceof ExistsQueryBuilder) == false) {
            // Query is not a simple term filter we can extract - fall back to regular execution
            logger.info("Star-tree optimization skipped: query has Lucene-level filter [{}] that star-tree can't handle", query.getName());
            return aggregateExec;
        }
        if (termQueryFilter != null) {
            logger.info("Star-tree: extracted term filter from query: {}", termQueryFilter);
        }

        // Find all available star-trees across target indices
        List<StarTreeInfo> allStarTrees = findAllStarTrees(context);
        if (allStarTrees.isEmpty()) {
            logger.debug("Star-tree optimization skipped: no star-tree found in target indices");
            return aggregateExec;
        }
        logger.debug("Star-tree optimization: found {} star-tree(s) to evaluate", allStarTrees.size());

        // Try each star-tree and find the best eligible one
        StarTreeInfo bestStarTree = null;
        StarTreeEligibilityChecker.EligibilityResult bestResult = null;
        List<EsStarTreeQueryExec.GroupingFieldFilter> bestFilters = null;
        int bestScore = Integer.MIN_VALUE;

        for (StarTreeInfo starTreeInfo : allStarTrees) {
            logger.debug(
                "Star-tree optimization: evaluating star-tree [{}] with groupingFields={} and values={}",
                starTreeInfo.name(),
                starTreeInfo.groupingFields(),
                starTreeInfo.values()
            );

            // Extract grouping field filters from FilterExec if present
            List<EsStarTreeQueryExec.GroupingFieldFilter> groupingFieldFilters = new ArrayList<>();
            if (filterExec != null) {
                GroupingFieldFilterExtractor.Result filterResult = GroupingFieldFilterExtractor.extract(
                    filterExec.condition(),
                    starTreeInfo.groupingFields
                );
                if (filterResult == null) {
                    // Filter contains conditions on non-grouping fields for this star-tree, try next
                    logger.debug("Star-tree [{}] skipped: filter has non-grouping field conditions", starTreeInfo.name());
                    continue;
                }
                groupingFieldFilters.addAll(filterResult.filters());
            }

            // Add TermQueryBuilder filter if present and the field is a grouping field
            if (termQueryFilter != null) {
                if (starTreeInfo.groupingFields.contains(termQueryFilter.groupingField()) == false) {
                    // TermQuery is on a non-grouping field - can't use this star-tree
                    logger.debug("Star-tree [{}] skipped: TermQuery field [{}] is not a grouping field",
                        starTreeInfo.name(), termQueryFilter.groupingField());
                    continue;
                }
                groupingFieldFilters.add(termQueryFilter);
            }

            // Check eligibility - use the new overload with full grouping field configs and eval mappings
            StarTreeEligibilityChecker.EligibilityResult result = StarTreeEligibilityChecker.checkEligibility(
                aggregateExec,
                starTreeInfo.groupingFieldConfigs,
                starTreeInfo.values,
                starTreeInfo.name,
                context,
                mode,
                evalFieldMappingsInfo
            );

            if (result.eligible() == false) {
                logger.debug("Star-tree [{}] skipped: eligibility check failed", starTreeInfo.name());
                continue;
            }

            // Calculate score for this star-tree
            Set<String> requiredGroupingFields = new HashSet<>(result.groupByFields());
            Set<String> requiredValueFields = new HashSet<>();
            for (var agg : result.aggregations()) {
                requiredValueFields.add(agg.valueField());
            }
            int score = starTreeInfo.scoreForQuery(requiredGroupingFields, requiredValueFields);

            logger.debug(
                "Star-tree [{}] eligible with score {}, groupByFields={}, aggregations={}",
                starTreeInfo.name(),
                score,
                result.groupByFields(),
                result.aggregations()
            );

            if (score > bestScore) {
                bestScore = score;
                bestStarTree = starTreeInfo;
                bestResult = result;
                bestFilters = groupingFieldFilters;
            }
        }

        if (bestResult == null) {
            logger.debug("Star-tree optimization skipped: no eligible star-tree found");
            return aggregateExec;
        }

        StarTreeEligibilityChecker.EligibilityResult result = bestResult;
        logger.debug(
            "Star-tree optimization: selected star-tree [{}] with score {}, groupByFields={}, aggregations={}",
            bestStarTree.name(),
            bestScore,
            result.groupByFields(),
            result.aggregations()
        );

        // Merge grouping field filters from eligibility check and filter extraction
        List<EsStarTreeQueryExec.GroupingFieldFilter> allFilters = new ArrayList<>(result.groupingFieldFilters());
        allFilters.addAll(bestFilters);

        logger.debug(
            "Transforming aggregation to star-tree query: starTree=[{}], mode={}, groupByFields={}, aggregations={}, index=[{}]",
            result.starTreeName(),
            mode,
            result.groupByFields(),
            result.aggregations(),
            queryExec.indexPattern()
        );

        // Determine output attributes based on aggregator mode
        List<Attribute> outputAttributes;
        if (mode == AggregatorMode.INITIAL) {
            // For INITIAL mode, we need to output intermediate attributes that match what
            // AggregateExec(FINAL) expects. Use the same logic as AbstractPhysicalOperationProviders.
            outputAttributes = AbstractPhysicalOperationProviders.intermediateAttributes(
                aggregateExec.aggregates(),
                aggregateExec.groupings()
            );
            logger.debug("Star-tree optimization: using intermediate attributes for INITIAL mode: {}", outputAttributes);
        } else {
            // For SINGLE mode, use final output attributes
            outputAttributes = result.outputAttributes();
        }

        // Create star-tree query execution
        return new EsStarTreeQueryExec(
            aggregateExec.source(),
            queryExec.indexPattern(),
            result.starTreeName(),
            queryExec.query(),
            queryExec.limit(),
            outputAttributes,
            result.groupByFields(),
            allFilters,
            result.aggregations(),
            mode
        );
    }

    /**
     * Extract a term query filter from a QueryBuilder.
     * Handles TermQueryBuilder, TermsQueryBuilder (IN clause), and wrappers like BoolQueryBuilder.
     *
     * @param query the QueryBuilder to extract from
     * @return the extracted filter, or null if not extractable
     */
    private static EsStarTreeQueryExec.GroupingFieldFilter extractTermQueryFilter(QueryBuilder query) {
        if (query == null) {
            return null;
        }

        // Direct TermQueryBuilder
        if (query instanceof TermQueryBuilder termQuery) {
            return extractFromTermQuery(termQuery);
        }

        // Direct TermsQueryBuilder (IN clause)
        if (query instanceof TermsQueryBuilder termsQuery) {
            return extractFromTermsQuery(termsQuery);
        }

        // SingleValueQuery wrapper (ES|QL uses this for single-value field checks)
        if (query instanceof SingleValueQuery.AbstractBuilder svqBuilder) {
            logger.info("Star-tree: extracting from SingleValueQuery wrapper, inner query: {}", svqBuilder.next().getClass().getSimpleName());
            return extractTermQueryFilter(svqBuilder.next());
        }

        // BoolQueryBuilder with term clauses
        if (query instanceof BoolQueryBuilder boolQuery) {
            logger.info("Star-tree: BoolQueryBuilder has {} filter, {} must, {} should, {} mustNot clauses",
                boolQuery.filter().size(), boolQuery.must().size(), boolQuery.should().size(), boolQuery.mustNot().size());

            // Try to extract from filter clauses first (preferred for ES|QL)
            for (QueryBuilder filterClause : boolQuery.filter()) {
                logger.info("Star-tree: checking filter clause: {}", filterClause.getClass().getSimpleName());
                EsStarTreeQueryExec.GroupingFieldFilter filter = extractTermQueryFilter(filterClause);
                if (filter != null) {
                    return filter;
                }
            }
            // Try must clauses
            for (QueryBuilder mustClause : boolQuery.must()) {
                logger.info("Star-tree: checking must clause: {} (full class: {})", mustClause.getClass().getSimpleName(), mustClause.getClass().getName());
                logger.info("Star-tree: must clause content: {}", mustClause);
                EsStarTreeQueryExec.GroupingFieldFilter filter = extractTermQueryFilter(mustClause);
                if (filter != null) {
                    return filter;
                }
            }
        }

        return null;
    }

    /**
     * Extract a filter from a TermQueryBuilder.
     */
    private static EsStarTreeQueryExec.GroupingFieldFilter extractFromTermQuery(TermQueryBuilder termQuery) {
        String fieldName = termQuery.fieldName();
        Object value = termQuery.value();

        if (value instanceof String strValue) {
            logger.info("Star-tree: extracted TermQuery filter: {} == \"{}\"", fieldName, strValue);
            return EsStarTreeQueryExec.GroupingFieldFilter.exactMatchString(fieldName, strValue);
        }
        // Non-string term query - can handle numeric values as ordinals
        logger.info("Star-tree: TermQueryBuilder has non-string value [{}] of type [{}]", value, value.getClass().getSimpleName());
        return null;
    }

    /**
     * Extract a filter from a TermsQueryBuilder (IN clause).
     */
    private static EsStarTreeQueryExec.GroupingFieldFilter extractFromTermsQuery(TermsQueryBuilder termsQuery) {
        String fieldName = termsQuery.fieldName();
        List<Object> values = termsQuery.values();

        if (values == null || values.isEmpty()) {
            return null;
        }

        // Extract string values
        List<String> stringValues = new ArrayList<>();
        for (Object value : values) {
            if (value instanceof String strValue) {
                stringValues.add(strValue);
            } else {
                // Non-string value in IN list - can't use star-tree for this
                logger.info("Star-tree: TermsQueryBuilder has non-string value [{}] of type [{}]", value, value.getClass().getSimpleName());
                return null;
            }
        }

        if (stringValues.isEmpty()) {
            return null;
        }

        logger.info("Star-tree: extracted TermsQuery filter: {} IN {}", fieldName, stringValues);
        return EsStarTreeQueryExec.GroupingFieldFilter.inStrings(fieldName, stringValues);
    }

    /**
     * Helper class to extract grouping field filters from WHERE clause expressions.
     */
    private static class GroupingFieldFilterExtractor {
        record Result(List<EsStarTreeQueryExec.GroupingFieldFilter> filters) {}

        static Result extract(Expression condition, Set<String> starTreeGroupingFields) {
            List<EsStarTreeQueryExec.GroupingFieldFilter> filters = new ArrayList<>();
            if (extractRecursive(condition, starTreeGroupingFields, filters)) {
                return new Result(filters);
            }
            return null; // Extraction failed
        }

        private static boolean extractRecursive(
            Expression expr,
            Set<String> starTreeGroupingFields,
            List<EsStarTreeQueryExec.GroupingFieldFilter> filters
        ) {
            if (expr instanceof And and) {
                // AND: both sides must be extractable
                return extractRecursive(and.left(), starTreeGroupingFields, filters)
                    && extractRecursive(and.right(), starTreeGroupingFields, filters);
            } else if (expr instanceof Or) {
                // OR is not supported for star-tree filters
                return false;
            } else if (expr instanceof Equals eq) {
                return extractEquality(eq.left(), eq.right(), starTreeGroupingFields, filters)
                    || extractEquality(eq.right(), eq.left(), starTreeGroupingFields, filters);
            } else if (expr instanceof GreaterThanOrEqual gte) {
                return extractRangeMin(gte.left(), gte.right(), starTreeGroupingFields, filters, true);
            } else if (expr instanceof GreaterThan gt) {
                return extractRangeMin(gt.left(), gt.right(), starTreeGroupingFields, filters, false);
            } else if (expr instanceof LessThanOrEqual lte) {
                return extractRangeMax(lte.left(), lte.right(), starTreeGroupingFields, filters, true);
            } else if (expr instanceof LessThan lt) {
                return extractRangeMax(lt.left(), lt.right(), starTreeGroupingFields, filters, false);
            } else if (expr instanceof In in) {
                // IN clause - extract string values for grouping field filters
                return extractInClause(in, starTreeGroupingFields, filters);
            }

            // Unknown expression type - check if it involves only grouping fields
            return false;
        }

        private static boolean extractEquality(
            Expression field,
            Expression value,
            Set<String> starTreeGroupingFields,
            List<EsStarTreeQueryExec.GroupingFieldFilter> filters
        ) {
            if (field instanceof FieldAttribute fa && value instanceof Literal lit) {
                String fieldName = fa.fieldName().string();
                if (starTreeGroupingFields.contains(fieldName)) {
                    Object val = lit.value();
                    if (val instanceof Number num) {
                        // Numeric equality - use ordinal directly
                        filters.add(EsStarTreeQueryExec.GroupingFieldFilter.exactMatch(fieldName, num.longValue()));
                        return true;
                    } else if (val instanceof String strVal) {
                        // String equality - store value for ordinal lookup at execution time
                        filters.add(EsStarTreeQueryExec.GroupingFieldFilter.exactMatchString(fieldName, strVal));
                        return true;
                    } else if (val instanceof org.apache.lucene.util.BytesRef bytesRef) {
                        // BytesRef (keyword field) - convert to string
                        filters.add(EsStarTreeQueryExec.GroupingFieldFilter.exactMatchString(fieldName, bytesRef.utf8ToString()));
                        return true;
                    }
                    // Unsupported value type - fall back to regular execution
                    return false;
                }
            }
            return false;
        }

        private static boolean extractRangeMin(
            Expression field,
            Expression value,
            Set<String> starTreeGroupingFields,
            List<EsStarTreeQueryExec.GroupingFieldFilter> filters,
            boolean inclusive
        ) {
            if (field instanceof FieldAttribute fa && value instanceof Literal lit) {
                String fieldName = fa.fieldName().string();
                if (starTreeGroupingFields.contains(fieldName)) {
                    Object val = lit.value();
                    if (val instanceof Number num) {
                        long min = inclusive ? num.longValue() : num.longValue() + 1;
                        filters.add(EsStarTreeQueryExec.GroupingFieldFilter.range(fieldName, min, null));
                        return true;
                    }
                }
            }
            return false;
        }

        private static boolean extractRangeMax(
            Expression field,
            Expression value,
            Set<String> starTreeGroupingFields,
            List<EsStarTreeQueryExec.GroupingFieldFilter> filters,
            boolean inclusive
        ) {
            if (field instanceof FieldAttribute fa && value instanceof Literal lit) {
                String fieldName = fa.fieldName().string();
                if (starTreeGroupingFields.contains(fieldName)) {
                    Object val = lit.value();
                    if (val instanceof Number num) {
                        long max = inclusive ? num.longValue() : num.longValue() - 1;
                        filters.add(EsStarTreeQueryExec.GroupingFieldFilter.range(fieldName, null, max));
                        return true;
                    }
                }
            }
            return false;
        }

        private static boolean extractInClause(
            In in,
            Set<String> starTreeGroupingFields,
            List<EsStarTreeQueryExec.GroupingFieldFilter> filters
        ) {
            Expression valueExpr = in.value();
            if (valueExpr instanceof FieldAttribute fa == false) {
                return false;
            }

            FieldAttribute fieldAttr = (FieldAttribute) valueExpr;
            String fieldName = fieldAttr.fieldName().string();
            if (starTreeGroupingFields.contains(fieldName) == false) {
                return false;
            }

            // Extract all string values from the IN list
            List<String> stringValues = new ArrayList<>();
            for (Expression listItem : in.list()) {
                if (listItem instanceof Literal lit) {
                    Object val = lit.value();
                    if (val instanceof String strVal) {
                        stringValues.add(strVal);
                    } else if (val instanceof org.apache.lucene.util.BytesRef bytesRef) {
                        stringValues.add(bytesRef.utf8ToString());
                    } else {
                        // Non-string value in IN list - can't use star-tree for this
                        return false;
                    }
                } else {
                    // Non-literal in IN list - can't use star-tree
                    return false;
                }
            }

            if (stringValues.isEmpty()) {
                return false;
            }

            filters.add(EsStarTreeQueryExec.GroupingFieldFilter.inStrings(fieldName, stringValues));
            return true;
        }

        private static boolean isGroupingField(Expression expr, Set<String> starTreeGroupingFields) {
            if (expr instanceof FieldAttribute fa) {
                return starTreeGroupingFields.contains(fa.fieldName().string());
            }
            return false;
        }
    }

    /**
     * Information about a field mapping from EvalExec.
     * When bucket() function is used, this captures the underlying field and the bucket interval.
     */
    record EvalFieldMappingInfo(String fieldName, long queryIntervalMillis) {
        /** Creates info for a non-date-histogram mapping (no interval). */
        static EvalFieldMappingInfo withoutInterval(String fieldName) {
            return new EvalFieldMappingInfo(fieldName, -1);
        }
        /** Creates info for a date-histogram mapping with an interval. */
        static EvalFieldMappingInfo withInterval(String fieldName, long intervalMillis) {
            return new EvalFieldMappingInfo(fieldName, intervalMillis);
        }
    }

    /**
     * Extract field mappings from EvalExec.
     * When bucket() function is used, ES|QL creates an EvalExec with ROUNDTO expression.
     * This method extracts the mapping from the alias name to the underlying field name,
     * and also computes the bucket interval from the rounding points.
     *
     * @param evalExec the EvalExec node (may be null)
     * @param context optimizer context for folding expressions
     * @return map from alias name to field mapping info
     */
    private static Map<String, EvalFieldMappingInfo> extractEvalFieldMappings(EvalExec evalExec, LocalPhysicalOptimizerContext context) {
        Map<String, EvalFieldMappingInfo> mappings = new HashMap<>();
        if (evalExec == null) {
            return mappings;
        }

        for (Alias alias : evalExec.fields()) {
            Expression child = alias.child();
            if (child instanceof RoundTo roundTo) {
                // RoundTo is used for bucket() - extract the underlying field
                Expression field = roundTo.field();
                if (field instanceof FieldAttribute fa) {
                    String aliasName = alias.name();
                    String fieldName = fa.fieldName().string();

                    // Try to compute the interval from the rounding points
                    long intervalMillis = computeIntervalFromPoints(roundTo.points(), context);
                    if (intervalMillis > 0) {
                        mappings.put(aliasName, EvalFieldMappingInfo.withInterval(fieldName, intervalMillis));
                        logger.debug("extractEvalFieldMappings: {} -> {} with interval {}ms", aliasName, fieldName, intervalMillis);
                    } else {
                        mappings.put(aliasName, EvalFieldMappingInfo.withoutInterval(fieldName));
                        logger.debug("extractEvalFieldMappings: {} -> {} (no interval)", aliasName, fieldName);
                    }
                }
            }
        }

        return mappings;
    }

    /**
     * Compute the bucket interval from the rounding points.
     * For date-based bucketing, the interval is the difference between consecutive points.
     *
     * @param points the list of rounding point expressions
     * @param context optimizer context for folding expressions
     * @return the interval in milliseconds, or -1 if it cannot be computed
     */
    private static long computeIntervalFromPoints(List<Expression> points, LocalPhysicalOptimizerContext context) {
        if (points == null || points.size() < 2) {
            return -1;
        }

        try {
            // Fold the first two points to get their values
            Object point0 = Foldables.valueOf(context.foldCtx(), points.get(0));
            Object point1 = Foldables.valueOf(context.foldCtx(), points.get(1));

            if (point0 instanceof Number n0 && point1 instanceof Number n1) {
                long interval = n1.longValue() - n0.longValue();
                if (interval > 0) {
                    return interval;
                }
            }
        } catch (Exception e) {
            logger.debug("Failed to compute interval from points: {}", e.getMessage());
        }

        return -1;
    }

    /**
     * Find all compatible star-tree configurations from the target indices.
     * Returns star-trees that are available (with compatible config) across all shards.
     */
    private List<StarTreeInfo> findAllStarTrees(LocalPhysicalOptimizerContext context) {
        Map<ShardId, IndexMetadata> targetShards = context.searchStats().targetShards();
        if (targetShards == null || targetShards.isEmpty()) {
            logger.debug("findAllStarTrees: no target shards found");
            return List.of();
        }
        logger.debug("findAllStarTrees: checking {} target shards", targetShards.size());

        // Collect star-trees from all shards and find the intersection
        List<StarTreeInfo> commonStarTrees = null;

        for (Map.Entry<ShardId, IndexMetadata> entry : targetShards.entrySet()) {
            IndexMetadata indexMetadata = entry.getValue();
            List<StarTreeInfo> shardStarTrees = extractAllStarTreeInfo(indexMetadata);

            if (shardStarTrees.isEmpty()) {
                // This index doesn't have any star-trees - can't use star-tree acceleration
                logger.debug("findAllStarTrees: index [{}] has no star-trees", indexMetadata.getIndex().getName());
                return List.of();
            }

            if (commonStarTrees == null) {
                commonStarTrees = new ArrayList<>(shardStarTrees);
            } else {
                // Keep only star-trees that are compatible across all shards
                commonStarTrees.removeIf(common -> {
                    for (StarTreeInfo shardTree : shardStarTrees) {
                        if (common.isCompatibleWith(shardTree)) {
                            return false; // Keep this tree - it's compatible
                        }
                    }
                    return true; // Remove this tree - no compatible version in this shard
                });
            }
        }

        if (commonStarTrees == null || commonStarTrees.isEmpty()) {
            logger.debug("findAllStarTrees: no common star-trees found across all shards");
            return List.of();
        }

        logger.debug("findAllStarTrees: found {} common star-trees: {}", commonStarTrees.size(),
            commonStarTrees.stream().map(StarTreeInfo::name).toList());
        return commonStarTrees;
    }

    /**
     * Extract all star-tree configurations from index metadata.
     * Returns empty list if extraction fails for any reason.
     */
    @SuppressWarnings("unchecked")
    private List<StarTreeInfo> extractAllStarTreeInfo(IndexMetadata indexMetadata) {
        List<StarTreeInfo> result = new ArrayList<>();

        try {
            return extractAllStarTreeInfoInternal(indexMetadata);
        } catch (Exception e) {
            // Gracefully handle any parsing errors - fall back to non-star-tree execution
            logger.debug(
                "extractAllStarTreeInfo: error parsing star-tree config for index [{}]: {}",
                indexMetadata.getIndex().getName(),
                e.getMessage()
            );
            return result;
        }
    }

    @SuppressWarnings("unchecked")
    private List<StarTreeInfo> extractAllStarTreeInfoInternal(IndexMetadata indexMetadata) {
        List<StarTreeInfo> result = new ArrayList<>();

        MappingMetadata mappingMetadata = indexMetadata.mapping();
        if (mappingMetadata == null) {
            logger.debug("extractAllStarTreeInfo: no mapping metadata for index [{}]", indexMetadata.getIndex().getName());
            return result;
        }

        Map<String, Object> mappingSource = mappingMetadata.sourceAsMap();
        if (mappingSource == null) {
            logger.debug("extractAllStarTreeInfo: mapping source is null for index [{}]", indexMetadata.getIndex().getName());
            return result;
        }
        logger.debug(
            "extractAllStarTreeInfo: mapping source keys = {} for index [{}]",
            mappingSource.keySet(),
            indexMetadata.getIndex().getName()
        );

        // Look for _star_tree configuration in the mapping
        Object starTreeObj = mappingSource.get("_star_tree");
        if (starTreeObj instanceof Map == false) {
            logger.debug(
                "extractAllStarTreeInfo: no _star_tree found in mapping for index [{}], value={}",
                indexMetadata.getIndex().getName(),
                starTreeObj
            );
            return result;
        }
        logger.debug("extractAllStarTreeInfo: found _star_tree config in index [{}]", indexMetadata.getIndex().getName());

        Map<String, Object> starTreeConfig = (Map<String, Object>) starTreeObj;
        if (starTreeConfig.isEmpty()) {
            return result;
        }

        // Iterate over all star-tree definitions
        for (Map.Entry<String, Object> entry : starTreeConfig.entrySet()) {
            String starTreeName = entry.getKey();
            if (entry.getValue() instanceof Map == false) {
                continue;
            }
            Map<String, Object> starTreeDef = (Map<String, Object>) entry.getValue();

            // Extract grouping fields with full configuration
            Set<String> groupingFields = new HashSet<>();
            Map<String, StarTreeEligibilityChecker.GroupingFieldConfig> groupingFieldConfigs = new HashMap<>();
            Object groupingFieldsObj = starTreeDef.get("grouping_fields");
            if (groupingFieldsObj instanceof List) {
                for (Object gfObj : (List<?>) groupingFieldsObj) {
                    if (gfObj instanceof Map) {
                        Map<String, Object> gfMap = (Map<String, Object>) gfObj;
                        Object field = gfMap.get("field");
                        if (field instanceof String fieldName) {
                            groupingFields.add(fieldName);

                            // Check for date_histogram type and interval
                            Object typeObj = gfMap.get("type");
                            if (typeObj instanceof String type && "date_histogram".equals(type)) {
                                Object intervalObj = gfMap.get("interval");
                                long intervalMillis = parseIntervalMillis(intervalObj);
                                if (intervalMillis > 0) {
                                    groupingFieldConfigs.put(
                                        fieldName,
                                        StarTreeEligibilityChecker.GroupingFieldConfig.dateHistogram(fieldName, intervalMillis)
                                    );
                                } else {
                                    groupingFieldConfigs.put(
                                        fieldName,
                                        StarTreeEligibilityChecker.GroupingFieldConfig.ordinal(fieldName)
                                    );
                                }
                            } else {
                                groupingFieldConfigs.put(
                                    fieldName,
                                    StarTreeEligibilityChecker.GroupingFieldConfig.ordinal(fieldName)
                                );
                            }
                        }
                    }
                }
            }

            // Extract values
            Set<String> values = new HashSet<>();
            Object valuesObj = starTreeDef.get("values");
            if (valuesObj instanceof List) {
                for (Object valueObj : (List<?>) valuesObj) {
                    if (valueObj instanceof Map) {
                        Map<String, Object> valueMap = (Map<String, Object>) valueObj;
                        Object field = valueMap.get("field");
                        if (field instanceof String) {
                            values.add((String) field);
                        }
                    }
                }
            }

            if (groupingFields.isEmpty() == false && values.isEmpty() == false) {
                result.add(new StarTreeInfo(starTreeName, groupingFields, groupingFieldConfigs, values));
                logger.debug(
                    "extractAllStarTreeInfo: found star-tree [{}] with groupingFields={}, values={}",
                    starTreeName,
                    groupingFields,
                    values
                );
            }
        }

        return result;
    }

    /**
     * Parse interval value to milliseconds.
     * Supports string format (e.g., "1h", "30m") or numeric milliseconds.
     */
    private static long parseIntervalMillis(Object intervalObj) {
        if (intervalObj instanceof Number num) {
            return num.longValue();
        } else if (intervalObj instanceof String intervalStr) {
            try {
                return org.elasticsearch.core.TimeValue.parseTimeValue(intervalStr, "interval").millis();
            } catch (Exception e) {
                return -1;
            }
        }
        return -1;
    }

    /**
     * Information about a star-tree configuration.
     */
    private record StarTreeInfo(
        String name,
        Set<String> groupingFields,
        Map<String, StarTreeEligibilityChecker.GroupingFieldConfig> groupingFieldConfigs,
        Set<String> values
    ) {
        boolean isCompatibleWith(StarTreeInfo other) {
            return name.equals(other.name) && groupingFields.equals(other.groupingFields) && values.equals(other.values);
        }

        /**
         * Calculate a score for how well this star-tree matches the query requirements.
         * Higher score means better match.
         *
         * @param requiredGroupingFields fields needed for GROUP BY
         * @param requiredValueFields fields needed for aggregations
         * @return score (negative if not usable, positive if usable)
         */
        int scoreForQuery(Set<String> requiredGroupingFields, Set<String> requiredValueFields) {
            // Check if all required grouping fields are covered
            if (groupingFields.containsAll(requiredGroupingFields) == false) {
                return -1; // Not usable - missing grouping fields
            }

            // Check if all required value fields are covered
            if (values.containsAll(requiredValueFields) == false) {
                return -1; // Not usable - missing value fields
            }

            // Score: prefer star-trees with fewer extra dimensions (more specific to query)
            // This helps choose a more focused star-tree when multiple are available
            int extraGroupingFields = groupingFields.size() - requiredGroupingFields.size();
            int extraValueFields = values.size() - requiredValueFields.size();

            // Base score of 100, minus penalties for extra fields
            return 100 - extraGroupingFields - extraValueFields;
        }
    }
}
