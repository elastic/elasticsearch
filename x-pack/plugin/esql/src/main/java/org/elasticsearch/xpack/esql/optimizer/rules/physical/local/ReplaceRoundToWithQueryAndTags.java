/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
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

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_NANOS;
import static org.elasticsearch.xpack.esql.core.type.DataTypeConverter.safeToLong;
import static org.elasticsearch.xpack.esql.planner.TranslatorHandler.TRANSLATOR_HANDLER;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.DEFAULT_DATE_NANOS_FORMATTER;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.DEFAULT_DATE_TIME_FORMATTER;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateNanosToLong;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateTimeToLong;

/**
 * {@code ReplaceRoundToWithQueryAndTags} builds a list of ranges and associated tags base on the rounding points defined in a
 * {@code RoundTo} function. It then rewrites the {@code EsQueryExec.query()} into a corresponding list of {@code QueryBuilder}s and tags,
 * each mapped to its respective range.
 *
 * Here are some examples:
 *
 * 1. Aggregation with {@code DateTrunc}.
 *    The {@code DateTrunc} function in the query below can be rewritten to {@code RoundTo} by {@code ReplaceDateTruncBucketWithRoundTo}.
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
 * 2. Aggregation with {@code DateTrunc} and the other pushdown functions
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
 * 3. Aggregation with {@code DateTrunc} and predicates on {@code DateTrunc} field
 *    When there are range predicates on the {@code RoundTo} field that can also be pushed down to Lucene, this rule combines the range
 *    query in the main query with the {@code RoundTo} ranges to create a list of {@code QueryBuilderAndTags}. The main query is then
 *    applied to each query leg.
 *    | WHERE date &gt;= "2023-10-20" and date &lt;= "2023-10-24"
 *    | STATS COUNT(*) BY d = DATE_TRUNC(1 day, date)
 *    becomes
 *    | WHERE date &gt;= "2023-10-20" and date &lt;= "2023-10-24"
 *    | EVAL d = ROUND_TO(hire_date, 1697760000000, 1697846400000, 1697932800000)
 *    | STATS COUNT(*) BY d
 *    becomes
 *    [QueryBuilderAndTags[query={
 *    "bool" : {
 *     "filter" : [
 *       {
 *         "esql_single_value" : {
 *           "field" : "date",
 *           "next" : {
 *             "range" : {
 *               "date" : {
 *                 "gt" : "2023-10-19T00:00:00.000Z",
 *                 "lt" : "2023-10-24T00:00:00.000Z",
 *                 "time_zone" : "Z",
 *                 "format" : "strict_date_optional_time",
 *                 "boost" : 0.0
 *               }
 *             }
 *           },
 *           "source" : "date > \"2023-10-19T00:00:00.000Z\"@2:9"
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
 *      "esql_single_value" : {
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
 *     },
 *     "source" : "date_trunc(1 day, date)@3:25"
 *   }
 *    }, tags=[1697846400000]], QueryBuilderAndTags[query={
 *   "bool" : {
 *     "filter" : [
 *       {
 *         "esql_single_value" : {
 *           "field" : "date",
 *           "next" : {
 *             "range" : {
 *               "date" : {
 *                 "gt" : "2023-10-19T00:00:00.000Z",
 *                 "lt" : "2023-10-24T00:00:00.000Z",
 *                 "time_zone" : "Z",
 *                 "format" : "strict_date_optional_time",
 *                 "boost" : 0.0
 *               }
 *             }
 *           },
 *           "source" : "date > \"2023-10-19T00:00:00.000Z\"@2:9"
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
 *    }, tags=[1697932800000]]]
 * There are some restrictions:
 * 1. Tags are not supported by {@code LuceneTopNSourceOperator}, if the sort is pushed down to Lucene, this rewrite does not apply.
 * 2. Tags are not supported by {@code TimeSeriesSourceOperator}, this rewrite does not apply to timeseries indices.
 * 3. Tags are not supported by {@code LuceneCountOperator}, this rewrite does not apply to {@code EsStatsQueryExec}, count with grouping
 *    is not supported by {@code EsStatsQueryExec} today.
 */
public class ReplaceRoundToWithQueryAndTags extends PhysicalOptimizerRules.ParameterizedOptimizerRule<
    EvalExec,
    LocalPhysicalOptimizerContext> {

    @Override
    protected PhysicalPlan rule(EvalExec evalExec, LocalPhysicalOptimizerContext ctx) {
        PhysicalPlan plan = evalExec;
        // TimeSeriesSourceOperator and LuceneTopNSourceOperator do not support QueryAndTags, skip them
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
                plan = planRoundTo(roundTos.get(0), evalExec, queryExec, ctx);
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
            new EsField(tagFieldName, roundTo.dataType(), Map.of(), false)
        );
        // Add new tag field to attributes/output
        List<Attribute> newAttributes = new ArrayList<>(queryExec.attrs());
        newAttributes.add(tagField);

        // create a new EsQueryExec with newAttributes/output and queryBuilderAndTags
        EsQueryExec queryExecWithTags = new EsQueryExec(
            queryExec.source(),
            queryExec.indexPattern(),
            queryExec.indexMode(),
            queryExec.indexNameWithModes(),
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
        // sort rounding points before building range queries on them
        List<? extends Number> points = resolveRoundingPoints(dataType, roundingPoints);
        if (points.size() != count || points.isEmpty()) {
            return null;
        }
        List<EsQueryExec.QueryBuilderAndTags> queries = new ArrayList<>(count);

        Source source = roundTo.source();
        Queries.Clause clause = queryExec.hasScoring() ? Queries.Clause.MUST : Queries.Clause.FILTER;
        Number tag = points.get(0);
        if (points.size() == 1) { // if there is only one rounding point, just tag the main query
            EsQueryExec.QueryBuilderAndTags queryBuilderAndTags = tagOnlyBucket(queryExec, tag);
            queries.add(queryBuilderAndTags);
        } else {
            Number lower = null;
            Number upper = null;
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
        }
        // if the field is nullable, build bucket with null tag
        if (isRoundToFieldNullable(queryExec.query(), field)) {
            queries.add(nullBucket(source, field, queryExec, pushdownPredicates, clause));
        }
        return queries;
    }

    /**
     * Sort the rounding points in ascending order.
     */
    private static List<? extends Number> resolveRoundingPoints(DataType dataType, List<Expression> roundingPoints) {
        return switch (dataType) {
            case LONG, DATETIME, DATE_NANOS -> sortedLongRoundingPoints(roundingPoints);
            case INTEGER -> sortedIntRoundingPoints(roundingPoints);
            case DOUBLE -> sortedDoubleRoundingPoints(roundingPoints);
            default -> List.of();
        };
    }

    private static List<Long> sortedLongRoundingPoints(List<Expression> roundingPoints) {
        List<Long> points = new ArrayList<>(roundingPoints.size());
        for (Expression e : roundingPoints) {
            if (e instanceof Literal l && l.value() instanceof Number n) {
                points.add(safeToLong(n));
            }
        }
        Collections.sort(points);
        return points;
    }

    private static List<Integer> sortedIntRoundingPoints(List<Expression> roundingPoints) {
        List<Integer> points = new ArrayList<>(roundingPoints.size());
        for (Expression e : roundingPoints) {
            if (e instanceof Literal l) {
                points.add((Integer) l.value());
            }
        }
        Collections.sort(points);
        return points;
    }

    private static List<Double> sortedDoubleRoundingPoints(List<Expression> roundingPoints) {
        List<Double> points = new ArrayList<>(roundingPoints.size());
        for (Expression e : roundingPoints) {
            if (e instanceof Literal l) {
                points.add((Double) l.value());
            }
        }
        Collections.sort(points);
        return points;
    }

    /**
     * Create a GTE, LT or range expression, and build the corresponding range queries.
     */
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
            // lower and upper should not be both null, should an assert be added here
            return new Range(source, field, lowerValue, true, upperValue, false, dataType.isDate() ? zoneId : null);
        }
    }

    /**
     * There is only one rounding point, just attach the tag/rounding point to the main query
     */
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
        QueryBuilder combinedQuery;
        if (mainQuery == null) {
            combinedQuery = Queries.combine(clause, List.of(newQuery));
        } else {
            combinedQuery = Queries.combine(clause, List.of(mainQuery, newQuery));
            if (newQuery instanceof SingleValueQuery.Builder newSingleValueQueryBuilder
                && newSingleValueQueryBuilder.next() instanceof RangeQueryBuilder roundToRangeQueryBuilder) {
                if (mainQuery instanceof SingleValueQuery.Builder mainSingleValueQueryBuilder) {
                    // mainQuery has only one subQuery, if it is a superset of roundToRangeQueryBuilder, replace it
                    QueryBuilder main = mainSingleValueQueryBuilder.next();
                    if (main instanceof RangeQueryBuilder mainRange) {
                        if (replaceRangeQueryInMainQuery(
                            mainRange.from(),
                            mainRange.to(),
                            mainRange.includeLower(),
                            mainRange.format(),
                            roundToRangeQueryBuilder.from(),
                            roundToRangeQueryBuilder.to(),
                            roundToRangeQueryBuilder.format()
                        )) {
                            combinedQuery = Queries.combine(clause, List.of(newQuery));
                        }
                    }
                } else if (mainQuery instanceof BoolQueryBuilder boolQueryBuilder) {
                    // try merging the subQueries of mainQuery with roundToRangeQueryBuilder
                    combinedQuery = mergeRangeQueriesOnRoundToField(clause, boolQueryBuilder, newSingleValueQueryBuilder);
                }
            }
        }
        return new EsQueryExec.QueryBuilderAndTags(combinedQuery, tags);
    }

    /**
     * Look for range queries on the RoundTo field in the main query, if it already covers the range of the roundTo range query, remove it
     * from the main query.
     */
    private static QueryBuilder mergeRangeQueriesOnRoundToField(
        Queries.Clause clause,
        BoolQueryBuilder originalBoolQueryBuilder,
        SingleValueQuery.Builder roundToRangeQueryBuilder
    ) {
        BoolQueryBuilder newBoolQueryBuilder = new BoolQueryBuilder();
        for (QueryBuilder q : originalBoolQueryBuilder.mustNot()) {
            newBoolQueryBuilder.mustNot(q);
        }
        for (QueryBuilder q : originalBoolQueryBuilder.should()) {
            newBoolQueryBuilder.should(q);
        }
        // Update the range query in the bool query if it is a superset of the range query in the roundTo
        boolean mainQueryModified = false;
        for (QueryBuilder queryBuilder : originalBoolQueryBuilder.must()) {
            if (queryBuilder instanceof SingleValueQuery.Builder mainSingleValueQueryBuilder
                && mainSingleValueQueryBuilder.next() instanceof RangeQueryBuilder main
                && roundToRangeQueryBuilder instanceof SingleValueQuery.Builder roundToSingleValueQueryBuilder
                && roundToSingleValueQueryBuilder.next() instanceof RangeQueryBuilder roundTo
                && main.fieldName().equalsIgnoreCase(roundTo.fieldName())) {
                if (replaceRangeQueryInMainQuery(
                    main.from(),
                    main.to(),
                    main.includeLower(),
                    main.format(),
                    roundTo.from(),
                    roundTo.to(),
                    roundTo.format()
                )) {
                    // The range query from main queries covers the range of the roundTo range, replace it with roundTo range query
                    newBoolQueryBuilder.must(roundToSingleValueQueryBuilder);
                    mainQueryModified = true;
                } else {
                    newBoolQueryBuilder.must(queryBuilder);
                }
            } else {
                newBoolQueryBuilder.must(queryBuilder);
            }
        }

        // TODO filter should be processed similarly as must, how to validate this code path?
        for (QueryBuilder queryBuilder : originalBoolQueryBuilder.filter()) {
            if (queryBuilder instanceof SingleValueQuery.Builder mainSingleValueQueryBuilder
                && mainSingleValueQueryBuilder.next() instanceof RangeQueryBuilder main
                && roundToRangeQueryBuilder instanceof SingleValueQuery.Builder roundToSingleValueQueryBuilder
                && roundToSingleValueQueryBuilder.next() instanceof RangeQueryBuilder roundTo
                && main.fieldName().equalsIgnoreCase(roundTo.fieldName())) {
                // If the range query from main queries covers the range of the roundTo range, remove it from the main queries
                if (replaceRangeQueryInMainQuery(
                    main.from(),
                    main.to(),
                    main.includeLower(),
                    main.format(),
                    roundTo.from(),
                    roundTo.to(),
                    roundTo.format()
                )) { // update the range query in the main query with the lower/upper/includeLower/includeUpper from the roundToQuery
                    newBoolQueryBuilder.filter(roundToSingleValueQueryBuilder);
                    mainQueryModified = true;
                } else {
                    newBoolQueryBuilder.filter(queryBuilder);
                }
            } else {
                newBoolQueryBuilder.filter(queryBuilder);
            }
        }
        return mainQueryModified
            ? newBoolQueryBuilder
            : Queries.combine(clause, List.of(originalBoolQueryBuilder, roundToRangeQueryBuilder));
    }

    private static boolean rangeQueriesOnTheSameField(QueryBuilder mainQueryBuilder, QueryBuilder roundToQueryBuilder) {
        return mainQueryBuilder instanceof SingleValueQuery.Builder mainSingleValueQueryBuilder
            && mainSingleValueQueryBuilder.next() instanceof RangeQueryBuilder main
            && roundToQueryBuilder instanceof SingleValueQuery.Builder roundToSingleValueQueryBuilder
            && roundToSingleValueQueryBuilder.next() instanceof RangeQueryBuilder roundTo
            && main.fieldName().equalsIgnoreCase(roundTo.fieldName());
    }

    /**
     * Update the range query in the main query if its range is a superset of the range in the roundTo range query
     */
    private static boolean replaceRangeQueryInMainQuery(
        Object mainLower,
        Object mainUpper,
        boolean mainIncludeLower,
        String mainFormat,
        Object roundToLower,
        Object roundToUpper,
        String roundToFormat
    ) {
        boolean isNumeric = mainFormat == null && roundToFormat == null;
        boolean isDate = false;
        boolean isDateNanos = false;

        if (isNumeric == false) { // check if this is a date or date_nanos
            isDate = mainFormat != null
                && mainFormat.equalsIgnoreCase(DEFAULT_DATE_TIME_FORMATTER.pattern())
                && roundToFormat != null
                && roundToFormat.equalsIgnoreCase(DEFAULT_DATE_TIME_FORMATTER.pattern());
            isDateNanos = mainFormat != null
                && mainFormat.equalsIgnoreCase(DEFAULT_DATE_NANOS_FORMATTER.pattern())
                && roundToFormat != null
                && roundToFormat.equalsIgnoreCase(DEFAULT_DATE_NANOS_FORMATTER.pattern());
        }

        if ((isNumeric || isDate || isDateNanos) == false) {
            return false;
        }

        // check lower bound
        if (mainLower != null) { // main has lower bound > or >=
            if (roundToLower == null) { // roundTo is <
                return false;
            } else { // roundTo has >=, it may also have upper bound
                int cmp;
                if (isNumeric && roundToLower instanceof Number roundTo && mainLower instanceof Number main) {
                    cmp = compare(roundTo, main);
                } else if (roundToLower instanceof String roundTo && mainLower instanceof String main) {
                    cmp = compare(roundTo, main, isDate ? DATETIME : DATE_NANOS);
                } else {
                    return false;
                }
                if (cmp < 0) {
                    return false;
                }
                if (cmp == 0 && mainIncludeLower == false) { // lower bound is the same, roundTo has >=, main has >
                    return false;
                }
            }
        }

        // check upper bound
        if (mainUpper != null) { // main has < or <=
            if (roundToUpper == null) { // roundTo is >=
                return false;
            } else { // roundTo has <, it may also have lower bound
                int cmp;
                if (isNumeric && roundToUpper instanceof Number roundTo && mainUpper instanceof Number main) {
                    cmp = compare(roundTo, main);
                } else if (roundToUpper instanceof String roundTo && mainUpper instanceof String main) {
                    cmp = compare(roundTo, main, isDate ? DATETIME : DATE_NANOS);
                } else {
                    return false;
                }
                return cmp <= 0;
            }
        }

        return true;
    }

    private static int compare(Number left, Number right) {
        double diff = left.doubleValue() - right.doubleValue();
        if (diff < 0) return -1;
        if (diff > 0) return 1;
        return 0;
    }

    private static int compare(String left, String right, DataType dataType) {
        Long leftValue = dataType == DATETIME ? dateTimeToLong(left) : dateNanosToLong(left);
        Long rightValue = dataType == DATETIME ? dateTimeToLong(right) : dateNanosToLong(right);
        long diff = leftValue - rightValue;
        if (diff < 0) return -1;
        if (diff > 0) return 1;
        return 0;
    }

    private static boolean isRoundToFieldNullable(QueryBuilder mainQuery, Expression roundToField) {
        String roundToFieldName = Expressions.name(roundToField);
        if (roundToFieldName == null) { // cannot find the field name ? treat it as nullable
            return true;
        }
        // if there is range or isNotNull predicate on the field, this field is not nullable
        if (hasRangeOrIsNotNull(mainQuery, roundToFieldName)) {
            return false;
        }
        // check the subQueries of bool query
        if (mainQuery instanceof BoolQueryBuilder boolQueryBuilder) {
            List<QueryBuilder> mainQueries = mainQueries(boolQueryBuilder);
            if (mainQueries.isEmpty() == false) {
                for (QueryBuilder queryBuilder : mainQueries) {
                    // if there is range or isNotNull predicate on the field, this field is not nullable
                    if (hasRangeOrIsNotNull(queryBuilder, roundToFieldName)) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    private static boolean hasRangeOrIsNotNull(QueryBuilder queryBuilder, String fieldName) {
        // Check if there is range or isNotNull predicate on the field
        return ((queryBuilder instanceof SingleValueQuery.Builder singleValueQueryBuilder
            && singleValueQueryBuilder.next() instanceof RangeQueryBuilder rangeQueryBuilder
            && rangeQueryBuilder.fieldName().equals(fieldName))
            || (queryBuilder instanceof ExistsQueryBuilder existsQueryBuilder && existsQueryBuilder.fieldName().equals(fieldName)));
    }

    /**
     * Extract subqueries from the main query according to the clause types.
     * SHOULD/OR is not supported, as only boolean term is considered.
     * MUST_NOT is not supported as it does not represent a range or not null predicate on a field.
     */
    private static List<QueryBuilder> mainQueries(BoolQueryBuilder boolQueryBuilder) {
        List<QueryBuilder> mainQueries = new ArrayList<>();
        mainQueries.addAll(boolQueryBuilder.must());
        mainQueries.addAll(boolQueryBuilder.filter());
        return mainQueries;
    }
}
