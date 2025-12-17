/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.compute.operator.lookup.LookupEnrichQueryGenerator;
import org.elasticsearch.compute.operator.lookup.QueryList;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.plan.physical.EsSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.stats.SearchContextStats;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.LOOKUP_JOIN_ON_BOOLEAN_EXPRESSION;
import static org.elasticsearch.xpack.esql.enrich.AbstractLookupService.termQueryList;
import static org.elasticsearch.xpack.esql.planner.TranslatorHandler.TRANSLATOR_HANDLER;

/**
 * A {@link LookupEnrichQueryGenerator} that combines multiple conditions into a single query list.
 * Each query in the resulting query will be a conjunction of all queries from the input lists at the same position.
 * In addition, we support an optional pre-join filter that will be applied to all queries if it is pushable.
 * If the pre-join filter cannot be pushed down to Lucene, it will be ignored.
 * This class is used in the context of a lookup join, where we need to generate a query for each row of the left dataset.
 * The query is then used to fetch the matching rows from the right dataset.
 * The class supports two types of joins:
 * 1. Field-based join: The join conditions are based on the equality of fields from the left and right datasets.
 * It is used for field-based join when the join is on more than one field or there is a preJoinFilter
 * 2. Expression-based join: The join conditions are based on a complex expression that can involve multiple fields and operators.
 */
public class ExpressionQueryList implements LookupEnrichQueryGenerator {
    private final List<QueryList> queryLists;
    private final List<Query> lucenePushableFilters = new ArrayList<>();
    private final SearchExecutionContext context;
    private final AliasFilter aliasFilter;
    private final LucenePushdownPredicates lucenePushdownPredicates;

    private ExpressionQueryList(
        List<QueryList> queryLists,
        SearchExecutionContext context,
        PhysicalPlan rightPreJoinPlan,
        ClusterService clusterService,
        AliasFilter aliasFilter
    ) {
        this.queryLists = new ArrayList<>(queryLists);
        this.context = context;
        this.aliasFilter = aliasFilter;
        this.lucenePushdownPredicates = LucenePushdownPredicates.from(
            SearchContextStats.from(List.of(context)),
            new EsqlFlags(clusterService.getClusterSettings())
        );
        buildPreJoinFilter(rightPreJoinPlan, clusterService);
    }

    /**
     * Creates a new {@link ExpressionQueryList} for a field-based join.
     * A field-based join is a join where the join conditions are based on the equality of fields from the left and right datasets.
     * For example | LOOKUP JOIN on field1, field2, field3
     * The query lists are generated from the join conditions.
     * The pre-join filter is an optional filter that is applied to the right dataset before the join.
     */
    public static ExpressionQueryList fieldBasedJoin(
        List<QueryList> queryLists,
        SearchExecutionContext context,
        PhysicalPlan rightPreJoinPlan,
        ClusterService clusterService,
        AliasFilter aliasFilter
    ) {
        if (queryLists.size() < 2 && (rightPreJoinPlan instanceof FilterExec == false)) {
            throw new IllegalArgumentException("ExpressionQueryList must have at least two QueryLists or a pre-join filter");
        }
        return new ExpressionQueryList(queryLists, context, rightPreJoinPlan, clusterService, aliasFilter);
    }

    /**
     * Creates a new {@link ExpressionQueryList} for an expression-based join.
     * An expression-based join is a join where the join conditions are based on a complex expression
     * that can involve multiple fields and operators.
     * Example | LOOKUP JOIN on left_field > right_field AND left_field2 == right_field2
     * The query lists are generated from the join conditions.
     * The pre-join filter is an optional filter that is applied to the right dataset before the join.
     */
    public static ExpressionQueryList expressionBasedJoin(
        SearchExecutionContext context,
        PhysicalPlan rightPreJoinPlan,
        ClusterService clusterService,
        LookupFromIndexService.TransportRequest request,
        AliasFilter aliasFilter,
        Warnings warnings
    ) {
        if (LOOKUP_JOIN_ON_BOOLEAN_EXPRESSION.isEnabled() == false) {
            throw new UnsupportedOperationException("Lookup Join on Boolean Expression capability is not enabled");
        }
        if (request.getJoinOnConditions() == null) {
            throw new IllegalStateException("expressionBasedJoin must have join conditions");
        }
        ExpressionQueryList expressionQueryList = new ExpressionQueryList(
            new ArrayList<>(),
            context,
            rightPreJoinPlan,
            clusterService,
            aliasFilter
        );
        expressionQueryList.buildJoinOnForExpressionJoin(request.getJoinOnConditions(), request.getMatchFields(), clusterService, warnings);
        return expressionQueryList;
    }

    private void buildJoinOnForExpressionJoin(
        Expression joinOnConditions,
        List<MatchConfig> matchFields,
        ClusterService clusterService,
        Warnings warnings
    ) {
        List<Expression> expressions = Predicates.splitAnd(joinOnConditions);
        for (Expression expr : expressions) {
            boolean applied = applyAsLeftRightBinaryComparison(expr, matchFields, clusterService, warnings);
            if (applied == false) {
                applied = applyAsRightSidePushableFilter(expr);
            }
            if (applied == false) {
                throw new IllegalArgumentException("Cannot apply join condition: " + expr);
            }
        }
    }

    private boolean applyAsRightSidePushableFilter(Expression filter) {
        if (filter instanceof TranslationAware translationAware) {
            if (TranslationAware.Translatable.YES.equals(translationAware.translatable(lucenePushdownPredicates))) {
                QueryBuilder queryBuilder = translationAware.asQuery(lucenePushdownPredicates, TRANSLATOR_HANDLER).toQueryBuilder();
                // Rewrite the query builder to ensure doIndexMetadataRewrite is called
                // Some functions, such as KQL require rewriting to work properly
                try {
                    queryBuilder = Rewriteable.rewrite(queryBuilder, context, true);
                } catch (IOException e) {
                    throw new UncheckedIOException("Error while rewriting query for Lucene pushable filter", e);
                }
                addToLucenePushableFilters(queryBuilder);
                return true;
            }
        }
        return false;
    }

    private boolean applyAsLeftRightBinaryComparison(
        Expression expr,
        List<MatchConfig> matchFields,
        ClusterService clusterService,
        Warnings warnings
    ) {
        if (expr instanceof EsqlBinaryComparison binaryComparison
            && binaryComparison.left() instanceof Attribute leftAttribute
            && binaryComparison.right() instanceof Attribute rightAttribute) {
            // the left side comes from the page that was sent to the lookup node
            // the right side is the field from the lookup index
            // check if the left side is in the matchFields
            DataType dataType = null;
            int channelOffset = -1;
            for (int i = 0; i < matchFields.size(); i++) {
                if (matchFields.get(i).fieldName().equals(leftAttribute.name())) {
                    channelOffset = i;
                    dataType = matchFields.get(i).type();
                    break;
                }
            }
            MappedFieldType rightFieldType = context.getFieldType(rightAttribute.name());
            if (rightFieldType != null && dataType != null && channelOffset != -1) {
                // special handle Equals operator
                // TermQuery is faster than BinaryComparisonQueryList, as it does less work per row
                // so here we reuse the existing logic from field based join to build a termQueryList for Equals
                if (binaryComparison instanceof Equals) {
                    QueryList termQueryForEquals = termQueryList(rightFieldType, context, aliasFilter, channelOffset, dataType);
                    queryLists.add(termQueryForEquals.onlySingleValues(warnings, "LOOKUP JOIN encountered multi-value"));
                } else {
                    ElementType elementType = PlannerUtils.toElementType(dataType);
                    queryLists.add(
                        new BinaryComparisonQueryList(
                            rightFieldType,
                            context,
                            elementType,
                            channelOffset,
                            binaryComparison,
                            clusterService,
                            aliasFilter,
                            warnings
                        )
                    );
                }
                return true;
            }
        }
        return false;
    }

    private void addToLucenePushableFilters(QueryBuilder query) {
        try {
            if (query != null) {
                lucenePushableFilters.add(query.toQuery(context));
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Error while building query for Lucene pushable filter", e);
        }
    }

    private void buildPreJoinFilter(PhysicalPlan rightPreJoinPlan, ClusterService clusterService) {
        if (rightPreJoinPlan instanceof FilterExec filterExec) {
            List<Expression> candidateRightHandFilters = Predicates.splitAnd(filterExec.condition());
            for (Expression filter : candidateRightHandFilters) {
                if (filter instanceof TranslationAware translationAware) {
                    if (TranslationAware.Translatable.YES.equals(translationAware.translatable(lucenePushdownPredicates))) {
                        addToLucenePushableFilters(translationAware.asQuery(lucenePushdownPredicates, TRANSLATOR_HANDLER).toQueryBuilder());
                    }
                }
                // If the filter is not translatable we will not apply it for now
                // as performance testing showed no performance improvement.
                // We can revisit this in the future if needed, once we have more optimized workflow in place.
                // The filter is optional, so it is OK to ignore it if it cannot be translated.
            }
        } else if (rightPreJoinPlan != null && rightPreJoinPlan instanceof EsSourceExec == false) {
            throw new IllegalStateException(
                "The right side of a LookupJoinExec can only be a FilterExec on top of an EsSourceExec or an EsSourceExec, but got: "
                    + rightPreJoinPlan
            );
        }
    }

    /**
     * Returns the query at the given position.
     * The query is a conjunction of all queries from the input lists at the same position.
     * If a pre-join filter exists, it is also added to the query.
     * @param position The position of the query to return.
     * @param inputPage The input page containing the values for the query lists.
     * @return The query at the given position, or null if any of the match fields are null.
     */
    @Override
    public Query getQuery(int position, Page inputPage) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (QueryList queryList : queryLists) {
            Query q = queryList.getQuery(position, inputPage);
            if (q == null) {
                // if any of the matchFields are null, it means there is no match for this position
                // A AND NULL is always NULL, so we can skip this position
                return null;
            }
            builder.add(q, BooleanClause.Occur.FILTER);
        }
        // also attach the pre-join filter if it exists
        for (Query preJoinFilter : lucenePushableFilters) {
            builder.add(preJoinFilter, BooleanClause.Occur.FILTER);
        }
        return builder.build();
    }

    /**
     * Returns the number of positions in the query list.
     * The number of positions is the same for all query lists.
     * @return The number of positions in the query list.
     * @throws IllegalArgumentException if the query lists have different position counts.
     */
    @Override
    public int getPositionCount(Page inputPage) {
        int positionCount = queryLists.get(0).getPositionCount(inputPage);
        for (QueryList queryList : queryLists) {
            if (queryList.getPositionCount(inputPage) != positionCount) {
                throw new IllegalArgumentException(
                    "All QueryLists must have the same position count, expected: "
                        + positionCount
                        + ", but got: "
                        + queryList.getPositionCount(inputPage)
                );
            }
        }
        return positionCount;
    }
}
