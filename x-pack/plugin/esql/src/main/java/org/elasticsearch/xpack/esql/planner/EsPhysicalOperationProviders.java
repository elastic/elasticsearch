/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.aggregation.BlockHash;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.lucene.LuceneDocRef;
import org.elasticsearch.compute.lucene.LuceneSourceOperator.LuceneSourceOperatorFactory;
import org.elasticsearch.compute.lucene.ValueSources;
import org.elasticsearch.compute.lucene.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.EmptySourceOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.OrdinalsGroupingOperator;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.DriverParallelism;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.LocalExecutionPlannerContext;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.PhysicalOperation;
import org.elasticsearch.xpack.ql.expression.Attribute;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.compute.lucene.LuceneSourceOperator.NO_LIMIT;

public class EsPhysicalOperationProviders extends AbstractPhysicalOperationProviders {

    private final List<SearchContext> searchContexts;

    public EsPhysicalOperationProviders(List<SearchContext> searchContexts) {
        this.searchContexts = searchContexts;
    }

    @Override
    public final PhysicalOperation fieldExtractPhysicalOperation(FieldExtractExec fieldExtractExec, PhysicalOperation source) {
        Layout.Builder layout = source.layout.builder();

        var sourceAttrs = fieldExtractExec.sourceAttributes();

        PhysicalOperation op = source;
        for (Attribute attr : fieldExtractExec.attributesToExtract()) {
            layout.appendChannel(attr.id());
            Layout previousLayout = op.layout;

            var sources = ValueSources.sources(searchContexts, attr.name(), LocalExecutionPlanner.toElementType(attr.dataType()));

            var luceneDocRef = new LuceneDocRef(
                previousLayout.getChannel(sourceAttrs.get(0).id()),
                previousLayout.getChannel(sourceAttrs.get(1).id()),
                previousLayout.getChannel(sourceAttrs.get(2).id())
            );

            op = op.with(
                new ValuesSourceReaderOperator.ValuesSourceReaderOperatorFactory(sources, luceneDocRef, attr.name()),
                layout.build()
            );
        }
        return op;
    }

    @Override
    public final PhysicalOperation sourcePhysicalOperation(EsQueryExec esQueryExec, LocalExecutionPlannerContext context) {
        Set<String> indices = esQueryExec.index().concreteIndices();
        List<SearchExecutionContext> matchedSearchContexts = searchContexts.stream()
            .filter(ctx -> indices.contains(ctx.indexShard().shardId().getIndexName()))
            .map(SearchContext::getSearchExecutionContext)
            .toList();
        LuceneSourceOperatorFactory operatorFactory = new LuceneSourceOperatorFactory(
            matchedSearchContexts,
            ctx -> ctx.toQuery(esQueryExec.query()).query(),
            context.dataPartitioning(),
            context.taskConcurrency(),
            esQueryExec.limit() != null ? (Integer) esQueryExec.limit().fold() : NO_LIMIT
        );
        Layout.Builder layout = new Layout.Builder();
        for (int i = 0; i < esQueryExec.output().size(); i++) {
            layout.appendChannel(esQueryExec.output().get(i).id());
        }
        if (operatorFactory.size() > 0) {
            context.driverParallelism(new DriverParallelism(DriverParallelism.Type.DATA_PARALLELISM, operatorFactory.size()));
            return PhysicalOperation.fromSource(operatorFactory, layout.build());
        } else {
            return PhysicalOperation.fromSource(new EmptySourceOperator.Factory(), layout.build());
        }
    }

    @Override
    public final Operator.OperatorFactory groupingOperatorFactory(
        LocalExecutionPlanner.PhysicalOperation source,
        AggregateExec aggregateExec,
        List<GroupingAggregator.GroupingAggregatorFactory> aggregatorFactories,
        Attribute attrSource,
        BlockHash.Type blockHashType,
        BigArrays bigArrays
    ) {
        var sourceAttributes = FieldExtractExec.extractSourceAttributesFrom(aggregateExec.child());
        var luceneDocRef = new LuceneDocRef(
            source.layout.getChannel(sourceAttributes.get(0).id()),
            source.layout.getChannel(sourceAttributes.get(1).id()),
            source.layout.getChannel(sourceAttributes.get(2).id())
        );
        // The grouping-by values are ready, let's group on them directly.
        // Costin: why are they ready and not already exposed in the layout?
        return new OrdinalsGroupingOperator.OrdinalsGroupingOperatorFactory(
            ValueSources.sources(searchContexts, attrSource.name(), LocalExecutionPlanner.toElementType(attrSource.dataType())),
            luceneDocRef,
            aggregatorFactories,
            BigArrays.NON_RECYCLING_INSTANCE
        );
    }
}
