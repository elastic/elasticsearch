/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.apache.lucene.search.Query;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.compute.operator.lookup.QueryList;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.stats.SearchContextStats;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.function.IntFunction;

public class BinaryComparisonQueryList extends QueryList {
    private final EsqlBinaryComparison binaryComparison;
    private final IntFunction<Object> blockValueReader;
    private final SearchExecutionContext searchExecutionContext;
    private final LucenePushdownPredicates lucenePushdownPredicates;

    public BinaryComparisonQueryList(
        MappedFieldType field,
        SearchExecutionContext searchExecutionContext,
        Block block,
        EsqlBinaryComparison binaryComparison,
        ClusterService clusterService,
        AliasFilter aliasFilter
    ) {
        super(field, searchExecutionContext, aliasFilter, block, null);
        // swap left and right if the field is on the right
        // We get a filter in the form left_expr >= right_expr
        // here we will swap it to right_expr <= left_expr
        // and later in doGetQuery we will replace left_expr with the value from the block
        this.binaryComparison = (EsqlBinaryComparison) binaryComparison.swapLeftAndRight();
        this.blockValueReader = QueryList.createBlockValueReader(block);
        this.searchExecutionContext = searchExecutionContext;
        lucenePushdownPredicates = LucenePushdownPredicates.from(
            SearchContextStats.from(List.of(searchExecutionContext)),
            new EsqlFlags(clusterService.getClusterSettings())
        );
    }

    @Override
    public QueryList onlySingleValues(Warnings warnings, String multiValueWarningMessage) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Query doGetQuery(int position, int firstValueIndex, int valueCount) {
        if (valueCount == 0) {
            return null;
        }
        Object value = blockValueReader.apply(firstValueIndex);
        // create a new comparison with the value from the block as a literal
        EsqlBinaryComparison comparison = binaryComparison.getFunctionType()
            .buildNewInstance(
                binaryComparison.source(),
                binaryComparison.left(),
                new Literal(binaryComparison.right().source(), value, binaryComparison.right().dataType())
            );
        try {
            return comparison.asQuery(lucenePushdownPredicates, TranslatorHandler.TRANSLATOR_HANDLER)
                .toQueryBuilder()
                .toQuery(searchExecutionContext);
        } catch (IOException e) {
            throw new UncheckedIOException("Error while building query for join on filter:", e);
        }
    }
}
