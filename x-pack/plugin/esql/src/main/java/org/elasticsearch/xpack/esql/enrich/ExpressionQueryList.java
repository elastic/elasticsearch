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
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.stats.SearchContextStats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.planner.TranslatorHandler.TRANSLATOR_HANDLER;

/**
 * A {@link LookupEnrichQueryGenerator} that combines multiple {@link QueryList}s into a single query.
 * Each query in the resulting query will be a conjunction of all queries from the input lists at the same position.
 * In the future we can extend this to support more complex expressions, such as disjunctions or negations.
 */
public class ExpressionQueryList implements LookupEnrichQueryGenerator {
    private static final Logger logger = LogManager.getLogger(ExpressionQueryList.class);
    private List<QueryList> queryLists;
    private final List<Query> preJoinFilters = new ArrayList<>();
    private final SearchExecutionContext context;
    boolean isExpressionJoin = false;
    private final AliasFilter aliasFilter;

    public ExpressionQueryList(
        List<QueryList> queryLists,
        SearchExecutionContext context,
        List<Expression> candidateRightHandFilters,
        ClusterService clusterService,
        LookupFromIndexService.TransportRequest request,
        AliasFilter aliasFilter
    ) {
        if (queryLists.size() < 2 && (candidateRightHandFilters == null || candidateRightHandFilters.isEmpty())) {
            throw new IllegalArgumentException("ExpressionQueryList must have at least two QueryLists");
        }
        this.queryLists = new ArrayList<>();
        this.context = context;
        this.aliasFilter = aliasFilter;
        buildPrePostJoinFilter(candidateRightHandFilters, clusterService, request);
        if (isExpressionJoin == false) {
            this.queryLists.addAll(queryLists);
        }

    }

    private void buildPrePostJoinFilter(
        List<Expression> candidateRightHandFilters,
        ClusterService clusterService,
        LookupFromIndexService.TransportRequest request
    ) {
        if (candidateRightHandFilters == null || candidateRightHandFilters.isEmpty()) {
            return; // no filters to apply
        }
        for (Expression filter : candidateRightHandFilters) {
            try {
                if (filter instanceof TranslationAware translationAware) {
                    LucenePushdownPredicates lucenePushdownPredicates = LucenePushdownPredicates.from(
                        SearchContextStats.from(List.of(context)),
                        new EsqlFlags(clusterService.getClusterSettings())
                    );
                    if (TranslationAware.Translatable.YES.equals(translationAware.translatable(lucenePushdownPredicates))) {
                        preJoinFilters.add(
                            translationAware.asQuery(lucenePushdownPredicates, TRANSLATOR_HANDLER).toQueryBuilder().toQuery(context)
                        );
                    } else {
                        applyFilterWithQueryList(filter, request, clusterService);
                    }
                } else {
                    applyFilterWithQueryList(filter, request, clusterService);
                }
                // If the filter is not translatable we will not apply it for now
                // as performance testing showed no performance improvement.
                // We can revisit this in the future if needed, once we have more optimized workflow in place.
                // The filter is optional, so it is OK to ignore it if it cannot be translated.
            } catch (IOException e) {
                // as the filter is optional an error in its application will be ignored
                logger.error(() -> "Failed to translate optional pre-join filter: [" + filter + "]", e);
            }
        }
    }

    private void applyFilterWithQueryList(
        Expression filter,
        LookupFromIndexService.TransportRequest request,
        ClusterService clusterService
    ) {
        if (filter instanceof EsqlBinaryComparison binaryComparison) {
            // the left side comes from the page that was sent to the lookup node
            // the right side is the field from the lookup index
            // check if the left side is in the request.getMatchFields()
            // if it is its corresponding page is the corresponding number in request.inputPage
            Expression left = binaryComparison.left();
            if (left instanceof Attribute leftAttribute) {
                for (int i = 0; i < request.getMatchFields().size(); i++) {
                    if (request.getMatchFields().get(i).fieldName().string().equals(leftAttribute.name())) {
                        Block block = request.getInputPage().getBlock(i);
                        Expression right = binaryComparison.right();
                        if (right instanceof Attribute rightAttribute) {
                            MappedFieldType fieldType = context.getFieldType(rightAttribute.name());
                            if (fieldType != null) {
                                isExpressionJoin = true;
                                queryLists.add(
                                    new BinaryComparisonQueryList(fieldType, context, block, binaryComparison, clusterService, aliasFilter)
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    @Override
    public Query getQuery(int position) throws IOException {
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
