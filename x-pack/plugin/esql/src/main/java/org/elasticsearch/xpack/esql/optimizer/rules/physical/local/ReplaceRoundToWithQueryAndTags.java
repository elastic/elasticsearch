/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.FuzzyQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.MultiTermQueryBuilder;
import org.elasticsearch.index.query.PrefixQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.RegexpQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.util.Queries;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.RoundTo;
import org.elasticsearch.xpack.esql.expression.predicate.Range;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.querydsl.query.SingleValueQuery;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.type.DataTypeConverter.safeToLong;
import static org.elasticsearch.xpack.esql.planner.TranslatorHandler.TRANSLATOR_HANDLER;
import static org.elasticsearch.xpack.esql.plugin.QueryPragmas.ROUNDTO_PUSHDOWN_THRESHOLD;

/**
 * {@code ReplaceRoundToWithQueryAndTags} builds a list of ranges and associated tags base on the rounding points defined in a
 * {@code RoundTo} function. It then rewrites the {@code EsQueryExec.query()} into a corresponding list of {@code QueryBuilder}s and tags,
 * each mapped to its respective range.
 *
 * Here are some examples:
 *
 * 1. Aggregation with date_histogram.
 *    The {@code DATE_TRUNC} function in the query below can be rewritten to {@code RoundTo} by {@code ReplaceDateTruncBucketWithRoundTo}.
 *    This rule pushes down the {@code RoundTo} function by creating a list of {@code QueryBuilderAndTags}, so that
 *    {@code EsPhysicalOperationProviders} can build {@code LuceneSliceQueue} with the corresponding list of {@code QueryAndTags} to process
 *    further.
 *    | STATS COUNT(*) BY d = DATE_TRUNC(1 day, date)
 *    becomes, the rounding points are calculated according to SearchStats and predicates from the query.
 *    | EVAL d = ROUND_TO(hire_date, 1697760000000, 1697846400000, 1697932800000)
 *    | STATS COUNT(*) BY d
 *    becomes
 *    [QueryBuilderAndTags[query={
 *     "esql_single_value" : {
 *      "field" : "date",
 *      "next" : {
 *       "range" : {
 *         "date" : {
 *           "lt" : "2023-10-21T00:00:00.000Z",
 *           "time_zone" : "Z",
 *           "format" : "strict_date_optional_time",
 *           "boost" : 0.0
 *         }
 *       }
 *      },
 *      "source" : "date_trunc(1 day, date)@2:25"
 *     }
 *    }, tags=[1697760000000]], QueryBuilderAndTags[query={
 *    "esql_single_value" : {
 *     "field" : "date",
 *     "next" : {
 *       "range" : {
 *         "date" : {
 *           "gte" : "2023-10-21T00:00:00.000Z",
 *           "lt" : "2023-10-22T00:00:00.000Z",
 *           "time_zone" : "Z",
 *           "format" : "strict_date_optional_time",
 *           "boost" : 0.0
 *         }
 *       }
 *      },
 *      "source" : "date_trunc(1 day, date)@2:25"
 *     }
 *    }, tags=[1697846400000]], QueryBuilderAndTags[query={
 *    "esql_single_value" : {
 *     "field" : "date",
 *     "next" : {
 *       "range" : {
 *         "date" : {
 *           "gte" : "2023-10-22T00:00:00.000Z",
 *           "time_zone" : "Z",
 *           "format" : "strict_date_optional_time",
 *           "boost" : 0.0
 *         }
 *       }
 *      },
 *      "source" : "date_trunc(1 day, date)@2:25"
 *     }
 *    }, tags=[1697932800000]], QueryBuilderAndTags[query={
 *    "bool" : {
 *     "must_not" : [
 *       {
 *         "exists" : {
 *           "field" : "date",
 *           "boost" : 0.0
 *         }
 *       }
 *     ],
 *     "boost" : 1.0
 *    }
 *   }, tags=[null]]]
 *
 * 2. Aggregation with date_histogram and the other pushdown functions
 *    When there are other functions that can also be pushed down to Lucene, this rule combines the main query with the {@code RoundTo}
 *    ranges to create a list of {@code QueryBuilderAndTags}. The main query is then applied to each query leg.
 *    | WHERE keyword : "keyword"
 *    | STATS COUNT(*) BY d = DATE_TRUNC(1 day, date)
 *    becomes
 *    | EVAL d = ROUND_TO(hire_date, 1697760000000, 1697846400000, 1697932800000)
 *    | STATS COUNT(*) BY d
 *    becomes
 *    [QueryBuilderAndTags[query={
 *    "bool" : {
 *     "filter" : [
 *       {
 *         "match" : {
 *           "keyword" : {
 *             "query" : "keyword",
 *             "lenient" : true
 *           }
 *         }
 *       },
 *       {
 *         "esql_single_value" : {
 *           "field" : "date",
 *           "next" : {
 *             "range" : {
 *               "date" : {
 *                 "lt" : "2023-10-21T00:00:00.000Z",
 *                 "time_zone" : "Z",
 *                 "format" : "strict_date_optional_time",
 *                 "boost" : 0.0
 *               }
 *             }
 *           },
 *           "source" : "date_trunc(1 day, date)@3:25"
 *         }
 *       }
 *     ],
 *     "boost" : 1.0
 *     }
 *    }, tags=[1697760000000]], QueryBuilderAndTags[query={
 *    "bool" : {
 *     "filter" : [
 *       {
 *         "match" : {
 *           "keyword" : {
 *             "query" : "keyword",
 *             "lenient" : true
 *           }
 *         }
 *       },
 *       {
 *         "esql_single_value" : {
 *           "field" : "date",
 *           "next" : {
 *             "range" : {
 *               "date" : {
 *                 "gte" : "2023-10-21T00:00:00.000Z",
 *                 "lt" : "2023-10-22T00:00:00.000Z",
 *                 "time_zone" : "Z",
 *                 "format" : "strict_date_optional_time",
 *                 "boost" : 0.0
 *               }
 *             }
 *           },
 *           "source" : "date_trunc(1 day, date)@3:25"
 *         }
 *       }
 *     ],
 *     "boost" : 1.0
 *     }
 *    }, tags=[1697846400000]], QueryBuilderAndTags[query={
 *   "bool" : {
 *     "filter" : [
 *       {
 *         "match" : {
 *           "keyword" : {
 *             "query" : "keyword",
 *             "lenient" : true
 *           }
 *         }
 *       },
 *       {
 *         "esql_single_value" : {
 *           "field" : "date",
 *           "next" : {
 *             "range" : {
 *               "date" : {
 *                 "gte" : "2023-10-22T00:00:00.000Z",
 *                 "time_zone" : "Z",
 *                 "format" : "strict_date_optional_time",
 *                 "boost" : 0.0
 *               }
 *             }
 *           },
 *           "source" : "date_trunc(1 day, date)@3:25"
 *         }
 *       }
 *     ],
 *     "boost" : 1.0
 *     }
 *    }, tags=[1697932800000]], QueryBuilderAndTags[query={
 *   "bool" : {
 *     "filter" : [
 *       {
 *         "match" : {
 *           "keyword" : {
 *             "query" : "keyword",
 *             "lenient" : true
 *           }
 *         }
 *       },
 *       {
 *         "bool" : {
 *           "must_not" : [
 *             {
 *               "exists" : {
 *                 "field" : "date",
 *                 "boost" : 0.0
 *               }
 *             }
 *           ],
 *           "boost" : 0.0
 *         }
 *       }
 *     ],
 *     "boost" : 1.0
 *     }
 *    }, tags=[null]]]
 *
 * There are some restrictions:
 * 1. Tags are not supported by {@code LuceneTopNSourceOperator}, if the sort is pushed down to Lucene, this rewrite does not apply.
 * 2. Tags are not supported by {@code TimeSeriesSourceOperator}, this rewrite does not apply to timeseries indices.
 * 3. Tags are not supported by {@code LuceneCountOperator}, this rewrite does not apply to {@code EsStatsQueryExec}, count with grouping
 *    is not supported by {@code EsStatsQueryExec} today.
 */
public class ReplaceRoundToWithQueryAndTags extends PhysicalOptimizerRules.ParameterizedOptimizerRule<
    EvalExec,
    LocalPhysicalOptimizerContext> {

    private static final Logger logger = LogManager.getLogger(ReplaceRoundToWithQueryAndTags.class);

    @Override
    protected PhysicalPlan rule(EvalExec evalExec, LocalPhysicalOptimizerContext ctx) {
        PhysicalPlan plan = evalExec;
        // TimeSeriesSourceOperator and LuceneTopNSourceOperator do not support QueryAndTags, skip them
        // Lookup join is not supported yet
        if (evalExec.child() instanceof EsQueryExec queryExec && queryExec.canSubstituteRoundToWithQueryBuilderAndTags()) {
            // Look for RoundTo and plan the push down for it.
            List<RoundTo> roundTos = evalExec.fields()
                .stream()
                .map(Alias::child)
                .filter(RoundTo.class::isInstance)
                .map(RoundTo.class::cast)
                .toList();
            // It is not clear how to push down multiple RoundTos, dealing with multiple RoundTos is out of the scope of this PR.
            if (roundTos.size() == 1) {
                RoundTo roundTo = roundTos.get(0);
                int count = roundTo.points().size();
                int roundingPointsUpperLimit = adjustedRoundingPointsThreshold(
                    ctx.searchStats(),
                    roundingPointsThreshold(ctx),
                    queryExec.query(),
                    queryExec.indexMode()
                );
                if (count > roundingPointsUpperLimit) {
                    logger.debug(
                        "Skipping RoundTo push down for [{}], as it has [{}] points, which is more than [{}]",
                        roundTo.source(),
                        count,
                        roundingPointsUpperLimit
                    );
                    return evalExec;
                }
                plan = planRoundTo(roundTo, evalExec, queryExec, ctx);
            }
        }
        return plan;
    }

    /**
     * Rewrite the {@code RoundTo} to a list of {@code QueryBuilderAndTags} as input to {@code EsPhysicalOperationProviders}.
     */
    private static PhysicalPlan planRoundTo(RoundTo roundTo, EvalExec evalExec, EsQueryExec queryExec, LocalPhysicalOptimizerContext ctx) {
        // Usually EsQueryExec has only one QueryBuilder, one Lucene query, without RoundTo push down.
        // If the RoundTo can be pushed down, a list of QueryBuilders with tags will be added into EsQueryExec, and it will be sent to
        // EsPhysicalOperationProviders.sourcePhysicalOperation to create a list of LuceneSliceQueue.QueryAndTags
        List<EsQueryExec.QueryBuilderAndTags> queryBuilderAndTags = queryBuilderAndTags(roundTo, queryExec, ctx);
        if (queryBuilderAndTags == null || queryBuilderAndTags.isEmpty()) {
            return evalExec;
        }

        FieldAttribute fieldAttribute = (FieldAttribute) roundTo.field();
        String tagFieldName = Attribute.rawTemporaryName(
            // $$fieldName$round_to$dateType
            fieldAttribute.fieldName().string(),
            "round_to",
            roundTo.field().dataType().typeName()
        );
        FieldAttribute tagField = new FieldAttribute(
            roundTo.source(),
            tagFieldName,
            new EsField(tagFieldName, roundTo.dataType(), Map.of(), false, EsField.TimeSeriesFieldType.NONE)
        );
        // Add new tag field to attributes/output
        List<Attribute> newAttributes = new ArrayList<>(queryExec.attrs());
        newAttributes.add(tagField);

        // create a new EsQueryExec with newAttributes/output and queryBuilderAndTags
        EsQueryExec queryExecWithTags = new EsQueryExec(
            queryExec.source(),
            queryExec.indexPattern(),
            queryExec.indexMode(),
            newAttributes,
            queryExec.limit(),
            queryExec.sorts(),
            queryExec.estimatedRowSize(),
            queryBuilderAndTags
        );

        // Replace RoundTo with new tag field in EvalExec
        List<Alias> updatedFields = evalExec.fields()
            .stream()
            .map(alias -> alias.child() instanceof RoundTo ? alias.replaceChild(tagField) : alias)
            .toList();

        return new EvalExec(evalExec.source(), queryExecWithTags, updatedFields);
    }

    private static List<EsQueryExec.QueryBuilderAndTags> queryBuilderAndTags(
        RoundTo roundTo,
        EsQueryExec queryExec,
        LocalPhysicalOptimizerContext ctx
    ) {
        LucenePushdownPredicates pushdownPredicates = LucenePushdownPredicates.from(ctx.searchStats(), ctx.flags());
        Expression field = roundTo.field();
        if (pushdownPredicates.isPushableFieldAttribute(field) == false) {
            return null;
        }
        List<Expression> roundingPoints = roundTo.points();
        int count = roundingPoints.size();
        DataType dataType = roundTo.dataType();
        // sort rounding points
        List<Object> points = resolveRoundingPoints(roundingPoints, dataType);
        if (points.size() != count || points.isEmpty()) {
            return null;
        }
        List<EsQueryExec.QueryBuilderAndTags> queries = new ArrayList<>(count);

        Object tag = points.get(0);
        if (points.size() == 1) { // if there is only one rounding point, just tag the main query
            EsQueryExec.QueryBuilderAndTags queryBuilderAndTags = tagOnlyBucket(queryExec, tag);
            queries.add(queryBuilderAndTags);
        } else {
            Source source = roundTo.source();
            Object lower = null;
            Object upper = null;
            Queries.Clause clause = queryExec.hasScoring() ? Queries.Clause.MUST : Queries.Clause.FILTER;
            ZoneId zoneId = ctx.configuration().zoneId();
            for (int i = 1; i < count; i++) {
                upper = points.get(i);
                // build predicates and range queries for RoundTo ranges
                queries.add(rangeBucket(source, field, dataType, lower, upper, tag, zoneId, queryExec, pushdownPredicates, clause));
                lower = upper;
                tag = upper;
            }
            // build the last/gte bucket
            queries.add(rangeBucket(source, field, dataType, lower, null, lower, zoneId, queryExec, pushdownPredicates, clause));
            // build null bucket
            queries.add(nullBucket(source, field, queryExec, pushdownPredicates, clause));
        }
        return queries;
    }

    private static List<Object> resolveRoundingPoints(List<Expression> roundingPoints, DataType dataType) {
        List<Object> points = new ArrayList<>(roundingPoints.size());
        for (Expression e : roundingPoints) {
            if (e instanceof Literal l && l.value() instanceof Number n) {
                switch (dataType) {
                    case INTEGER -> points.add(n.intValue());
                    case LONG, DATETIME, DATE_NANOS -> points.add(safeToLong(n));
                    case DOUBLE -> points.add(n.doubleValue());
                    // this should not happen, as RoundTo type resolution will fail with the other data types
                    default -> throw new IllegalArgumentException("Unsupported data type: " + dataType);
                }
            }
        }
        return RoundTo.sortedRoundingPoints(points, dataType);
    }

    private static Expression createRangeExpression(
        Source source,
        Expression field,
        DataType dataType,
        Object lower,
        Object upper,
        ZoneId zoneId
    ) {
        Literal lowerValue = new Literal(source, lower, dataType);
        Literal upperValue = new Literal(source, upper, dataType);
        if (lower == null) {
            return new LessThan(source, field, upperValue, zoneId);
        } else if (upper == null) {
            return new GreaterThanOrEqual(source, field, lowerValue, zoneId);
        } else {
            // lower and upper should not be both null
            return new Range(source, field, lowerValue, true, upperValue, false, dataType.isDate() ? zoneId : null);
        }
    }

    private static EsQueryExec.QueryBuilderAndTags tagOnlyBucket(EsQueryExec queryExec, Object tag) {
        return new EsQueryExec.QueryBuilderAndTags(queryExec.query(), List.of(tag));
    }

    private static EsQueryExec.QueryBuilderAndTags nullBucket(
        Source source,
        Expression field,
        EsQueryExec queryExec,
        LucenePushdownPredicates pushdownPredicates,
        Queries.Clause clause
    ) {
        IsNull isNull = new IsNull(source, field);
        List<Object> nullTags = new ArrayList<>(1);
        nullTags.add(null);
        return buildCombinedQueryAndTags(queryExec, pushdownPredicates, isNull, clause, nullTags);
    }

    private static EsQueryExec.QueryBuilderAndTags rangeBucket(
        Source source,
        Expression field,
        DataType dataType,
        Object lower,
        Object upper,
        Object tag,
        ZoneId zoneId,
        EsQueryExec queryExec,
        LucenePushdownPredicates pushdownPredicates,
        Queries.Clause clause
    ) {
        Expression range = createRangeExpression(source, field, dataType, lower, upper, zoneId);
        return buildCombinedQueryAndTags(queryExec, pushdownPredicates, range, clause, List.of(tag));
    }

    private static EsQueryExec.QueryBuilderAndTags buildCombinedQueryAndTags(
        EsQueryExec queryExec,
        LucenePushdownPredicates pushdownPredicates,
        Expression expression,
        Queries.Clause clause,
        List<Object> tags
    ) {
        Query queryDSL = TRANSLATOR_HANDLER.asQuery(pushdownPredicates, expression);
        QueryBuilder mainQuery = queryExec.query();
        QueryBuilder newQuery = queryDSL.toQueryBuilder();
        QueryBuilder combinedQuery = Queries.combine(clause, mainQuery != null ? List.of(mainQuery, newQuery) : List.of(newQuery));
        return new EsQueryExec.QueryBuilderAndTags(combinedQuery, tags);
    }

    /**
     * Get the rounding points upper limit for {@code RoundTo} pushdown from query level pragmas or cluster level flags.
     * If the query level pragmas is set to -1(default), the cluster level flags will be used.
     * If the query level pragmas is set to greater than or equals to 0, the query level pragmas will be used.
     */
    private int roundingPointsThreshold(LocalPhysicalOptimizerContext ctx) {
        int queryLevelRoundingPointsThreshold = ctx.configuration().pragmas().roundToPushDownThreshold();
        int clusterLevelRoundingPointsThreshold = ctx.flags().roundToPushdownThreshold();
        int roundingPointsThreshold;
        if (queryLevelRoundingPointsThreshold == ROUNDTO_PUSHDOWN_THRESHOLD.getDefault(ctx.configuration().pragmas().getSettings())) {
            roundingPointsThreshold = clusterLevelRoundingPointsThreshold;
        } else {
            roundingPointsThreshold = queryLevelRoundingPointsThreshold;
        }
        return roundingPointsThreshold;
    }

    /**
     * If the main query is expensive (such as including wildcard queries), executing more queries with tags is slower and more costly
     * than executing fewer queries without tags and then reading points and rounding. The rounding points threshold is treated as the
     * maximum number of clauses allowed to execute. We estimate the number of clauses in the main query and adjust the threshold so
     * that the total number of clauses does not exceed the limit by too much. Some expensive queries count as more than one clause;
     * for example, a wildcard query counts as 5 clauses, and a terms query counts as the number of terms.
     */
    static int adjustedRoundingPointsThreshold(SearchStats stats, int threshold, QueryBuilder query, IndexMode indexMode) {
        int clauses = estimateQueryClauses(stats, query) + 1;
        if (indexMode == IndexMode.TIME_SERIES) {
            // No doc partitioning for time_series sources; increase the threshold to trade overhead for parallelism.
            threshold *= 2;
        }
        return Math.ceilDiv(threshold, clauses);
    }

    static int estimateQueryClauses(SearchStats stats, QueryBuilder q) {
        if (q == null || q instanceof MatchAllQueryBuilder || q instanceof MatchNoneQueryBuilder) {
            return 0;
        }
        if (q instanceof WildcardQueryBuilder
            || q instanceof RegexpQueryBuilder
            || q instanceof PrefixQueryBuilder
            || q instanceof FuzzyQueryBuilder) {
            return 5;
        }
        if (q instanceof RangeQueryBuilder r) {
            // with points count 1, without count 3
            return stats.min(new FieldAttribute.FieldName(r.fieldName())) != null ? 1 : 3;
        }
        if (q instanceof MultiTermQueryBuilder) {
            return 3;
        }
        if (q instanceof TermsQueryBuilder terms && terms.values() != null) {
            return terms.values().size();
        }
        if (q instanceof SingleValueQuery.Builder b) {
            // ignore the single_value clause
            return Math.max(1, estimateQueryClauses(stats, b.next()));
        }
        if (q instanceof BoolQueryBuilder bq) {
            int total = 0;
            for (var c : bq.filter()) {
                total += estimateQueryClauses(stats, c);
            }
            for (var c : bq.must()) {
                total += estimateQueryClauses(stats, c);
            }
            for (var c : bq.should()) {
                total += estimateQueryClauses(stats, c);
            }
            for (var c : bq.mustNot()) {
                total += Math.max(2, estimateQueryClauses(stats, c));
            }
            return total;
        }
        return 1;
    }
}
