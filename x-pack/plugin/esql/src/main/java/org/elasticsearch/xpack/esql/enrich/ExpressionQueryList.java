/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.operator.lookup.LookupEnrichQueryGenerator;
import org.elasticsearch.compute.operator.lookup.QueryList;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.UnaryExec;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.stats.SearchContextStats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.planner.TranslatorHandler.TRANSLATOR_HANDLER;

/**
 * A {@link LookupEnrichQueryGenerator} that combines one or more {@link QueryList}s into a single query.
 * Each query in the resulting query will be a conjunction of all queries from the input lists at the same position.
 * In addition, we support an optional pre-join filter that will be applied to all queries if it is pushable.
 * If the pre-join filter cannot be pushed down to Lucene, it will be ignored.
 */
public class ExpressionQueryList implements LookupEnrichQueryGenerator {
    private static final Logger logger = LogManager.getLogger(ExpressionQueryList.class);
    private final List<QueryList> queryLists;
    private final List<Query> preJoinFilters = new ArrayList<>();
    private final SearchExecutionContext context;
    boolean isExpressionJoin = false;
    private final AliasFilter aliasFilter;

    public ExpressionQueryList(
        List<QueryList> queryLists,
        SearchExecutionContext context,
        PhysicalPlan rightPreJoinPlan,
        ClusterService clusterService,
        LookupFromIndexService.TransportRequest request,
        AliasFilter aliasFilter
    ) {
        if (queryLists.size() < 2 && (rightPreJoinPlan instanceof FilterExec == false) && request.getJoinOnConditions() == null) {
            throw new IllegalArgumentException("ExpressionQueryList must have at least two QueryLists or a pre-join filter");
        }
        this.queryLists = queryLists;
        this.context = context;
        this.aliasFilter = aliasFilter;
        buildJoinOnConditions(request, clusterService);
        buildPreJoinFilter(rightPreJoinPlan, clusterService);

    }

    private void buildJoinOnConditions(LookupFromIndexService.TransportRequest request, ClusterService clusterService) {
        // we support 2 modes of operation:
        // Join on fields
        // Join on AND of binary comparisons
        Expression filter = request.getJoinOnConditions();
        if (filter == null) {
            // the join on conditions are already populated via the queryLists
            // there is nothing to do here
            return;
        } else {
            // clear the join on conditions in the query lists
            // the join on condition needs to come from the expression
            queryLists.clear();
        }
        List<Expression> expressions = Predicates.splitAnd(filter);
        for (Expression expr : expressions) {
            if (expr instanceof EsqlBinaryComparison binaryComparison) {
                // the left side comes from the page that was sent to the lookup node
                // the right side is the field from the lookup index
                // check if the left side is in the request.getMatchFields()
                // if it is its corresponding page is the corresponding number in request.inputPage
                Expression left = binaryComparison.left();
                if (left instanceof Attribute leftAttribute) {
                    boolean matched = false;
                    for (int i = 0; i < request.getMatchFields().size(); i++) {
                        if (request.getMatchFields().get(i).fieldName().string().equals(leftAttribute.name())) {
                            Block block = request.getInputPage().getBlock(i);
                            Expression right = binaryComparison.right();
                            if (right instanceof Attribute rightAttribute) {
                                MappedFieldType fieldType = context.getFieldType(rightAttribute.name());
                                if (fieldType != null) {
                                    isExpressionJoin = true;
                                    queryLists.add(
                                        new BinaryComparisonQueryList(
                                            fieldType,
                                            context,
                                            block,
                                            binaryComparison,
                                            clusterService,
                                            aliasFilter
                                        )
                                    );
                                    matched = true;
                                    break;
                                } else {
                                    throw new IllegalArgumentException(
                                        "Could not find field [" + rightAttribute.name() + "] in the lookup join index"
                                    );
                                }
                            } else {
                                throw new IllegalArgumentException(
                                    "Only field from the right dataset are supported on the right of the join on condition but got: " + expr
                                );
                            }
                        }
                    }
                    if (matched == false) {
                        throw new IllegalArgumentException(
                            "Could not find field [" + leftAttribute.name() + "] in the left side of the lookup join"
                        );
                    }
                } else {
                    throw new IllegalArgumentException(
                        "Only field from the left dataset are supported on the left of the join on condition but got: " + expr
                    );
                }
            } else {
                // we only support binary comparisons in the join on conditions
                throw new IllegalArgumentException("Only binary comparisons are supported in join ON conditions, but got: " + expr);
            }
        }
    }

    private void addToPreJoinFilters(org.elasticsearch.index.query.QueryBuilder query) {
        try {
            if (query != null) {
                preJoinFilters.add(query.toQuery(context));
            }
        } catch (IOException e) {
            // as we treat the filter as optional an error in its application will be ignored
            logger.error(() -> "Failed to translate optional pre-join filter: [" + query + "]", e);
        }
    }

    private void buildPreJoinFilter(PhysicalPlan rightPreJoinPlan, ClusterService clusterService) {
        if (rightPreJoinPlan instanceof EsQueryExec esQueryExec) {
            // this does not happen right now, as we only do local mapping on the lookup node
            // so we have EsSourceExec, not esQueryExec
            if (esQueryExec.query() != null) {
                addToPreJoinFilters(esQueryExec.query());
            }
        } else if (rightPreJoinPlan instanceof FilterExec filterExec) {
            List<Expression> candidateRightHandFilters = Predicates.splitAnd(filterExec.condition());
            LucenePushdownPredicates lucenePushdownPredicates = LucenePushdownPredicates.from(
                SearchContextStats.from(List.of(context)),
                new EsqlFlags(clusterService.getClusterSettings())
            );
            for (Expression filter : candidateRightHandFilters) {
                if (filter instanceof TranslationAware translationAware) {
                    if (TranslationAware.Translatable.YES.equals(translationAware.translatable(lucenePushdownPredicates))) {
                        addToPreJoinFilters(translationAware.asQuery(lucenePushdownPredicates, TRANSLATOR_HANDLER).toQueryBuilder());
                    }
                }
                // If the filter is not translatable we will not apply it for now
                // as performance testing showed no performance improvement.
                // We can revisit this in the future if needed, once we have more optimized workflow in place.
                // The filter is optional, so it is OK to ignore it if it cannot be translated.
            }
            // call recursively to find other filters that might be present
            // either in another FilterExec or in an EsQueryExec
            buildPreJoinFilter(filterExec.child(), clusterService);
        } else if (rightPreJoinPlan instanceof UnaryExec unaryExec) {
            // there can be other nodes in the plan such as FieldExtractExec in the future
            buildPreJoinFilter(unaryExec.child(), clusterService);
        }
        // else we do nothing, as the filters are optional and we don't want to fail the query if there are any errors
        // this also covers the case of rightPreJoinPlan being null
    }

    @Override
    public Query getQuery(int position) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (QueryList queryList : queryLists) {
            Query q = queryList.getQuery(position);
            if (q == null) {
                // if any of the matchFields are null, it means there is no match for this position
                // A AND NULL is always NULL, so we can skip this position
                return null;
            }
            builder.add(q, BooleanClause.Occur.FILTER);
        }
        // also attach the pre-join filter if it exists
        for (Query preJoinFilter : preJoinFilters) {
            builder.add(preJoinFilter, BooleanClause.Occur.FILTER);
        }
        return builder.build();
    }

    @Override
    public int getPositionCount() {
        int positionCount = queryLists.get(0).getPositionCount();
        for (QueryList queryList : queryLists) {
            if (queryList.getPositionCount() != positionCount) {
                throw new IllegalArgumentException(
                    "All QueryLists must have the same position count, expected: "
                        + positionCount
                        + ", but got: "
                        + queryList.getPositionCount()
                );
            }
        }
        return positionCount;
    }
}
