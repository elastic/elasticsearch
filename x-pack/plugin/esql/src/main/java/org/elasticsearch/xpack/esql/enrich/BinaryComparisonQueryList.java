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
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.stats.SearchContextStats;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.function.BiFunction;

/**
 * A {@link QueryList} that generates a query for a binary comparison.
 * This class is used in the context of an expression based lookup join,
 * where we need to generate a query for each row of the left dataset.
 * The query is then used to fetch the matching rows from the right dataset.
 * The query is a binary comparison between a field from the right dataset
 * and a single value from the left dataset. e.g right_id > 5
 * The value is extracted from a block at a given position.
 * The comparison is then translated to a Lucene query.
 * If the comparison cannot be translated, an exception is thrown.
 * This class is used in conjunction with {@link ExpressionQueryList} to generate the final query for the lookup join.
 */
public class BinaryComparisonQueryList extends QueryList {
    private final EsqlBinaryComparison binaryComparison;
    private final BiFunction<Block, Integer, Object> blockValueReader;
    private final SearchExecutionContext searchExecutionContext;
    private final LucenePushdownPredicates lucenePushdownPredicates;

    public BinaryComparisonQueryList(
        MappedFieldType field,
        SearchExecutionContext searchExecutionContext,
        Block leftHandSideBlock,
        int channelOffset,
        EsqlBinaryComparison binaryComparison,
        ClusterService clusterService,
        AliasFilter aliasFilter,
        Warnings warnings
    ) {
        super(
            field,
            searchExecutionContext,
            aliasFilter,
            channelOffset,
            new OnlySingleValueParams(warnings, "LOOKUP JOIN encountered multi-value")
        );
        // swap left and right if the field is on the right
        // We get a filter in the form left_expr >= right_expr
        // here we will swap it to right_expr <= left_expr
        // and later in doGetQuery we will replace left_expr with the value from the leftHandSideBlock
        // We do that because binaryComparison expects the field to be on the left and the literal on the right to be translatable
        this.binaryComparison = (EsqlBinaryComparison) binaryComparison.swapLeftAndRight();
        this.blockValueReader = QueryList.createBlockValueReaderForType(leftHandSideBlock.elementType());
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
    public Query doGetQuery(int position, int firstValueIndex, int valueCount, Block inputBlock) {
        Object value = blockValueReader.apply(inputBlock, firstValueIndex);
        // create a new comparison with the value from the block as a literal
        EsqlBinaryComparison comparison = binaryComparison.getFunctionType()
            .buildNewInstance(
                binaryComparison.source(),
                binaryComparison.left(),
                new Literal(binaryComparison.right().source(), value, binaryComparison.right().dataType())
            );
        try {
            if (TranslationAware.Translatable.YES == comparison.translatable(lucenePushdownPredicates)) {
                return comparison.asQuery(lucenePushdownPredicates, TranslatorHandler.TRANSLATOR_HANDLER)
                    .toQueryBuilder()
                    .toQuery(searchExecutionContext);
            } else {
                throw new IllegalStateException("Cannot translate join condition: " + binaryComparison);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Error while building query for join on filter:", e);
        }
    }
}
