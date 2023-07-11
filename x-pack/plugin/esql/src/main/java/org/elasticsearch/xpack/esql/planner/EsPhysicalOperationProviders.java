/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.lucene.LuceneOperator;
import org.elasticsearch.compute.lucene.LuceneSourceOperator.LuceneSourceOperatorFactory;
import org.elasticsearch.compute.lucene.LuceneTopNSourceOperator.LuceneTopNSourceOperatorFactory;
import org.elasticsearch.compute.lucene.ValueSources;
import org.elasticsearch.compute.lucene.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.EmptySourceOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.OrdinalsGroupingOperator;
import org.elasticsearch.index.mapper.NestedLookup;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.search.NestedHelper;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec.FieldSort;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.DriverParallelism;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.LocalExecutionPlannerContext;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.PhysicalOperation;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Attribute;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.common.lucene.search.Queries.newNonNestedFilter;
import static org.elasticsearch.compute.lucene.LuceneSourceOperator.NO_LIMIT;

public class EsPhysicalOperationProviders extends AbstractPhysicalOperationProviders {

    private final List<SearchContext> searchContexts;

    public EsPhysicalOperationProviders(List<SearchContext> searchContexts) {
        this.searchContexts = searchContexts;
    }

    @Override
    public final PhysicalOperation fieldExtractPhysicalOperation(FieldExtractExec fieldExtractExec, PhysicalOperation source) {
        Layout.Builder layout = source.layout.builder();

        var sourceAttr = fieldExtractExec.sourceAttribute();

        PhysicalOperation op = source;
        for (Attribute attr : fieldExtractExec.attributesToExtract()) {
            layout.appendChannel(attr.id());
            Layout previousLayout = op.layout;

            var sources = ValueSources.sources(
                searchContexts,
                attr.name(),
                EsqlDataTypes.isUnsupported(attr.dataType()),
                LocalExecutionPlanner.toElementType(attr.dataType())
            );

            int docChannel = previousLayout.getChannel(sourceAttr.id());

            op = op.with(
                new ValuesSourceReaderOperator.ValuesSourceReaderOperatorFactory(sources, docChannel, attr.name()),
                layout.build()
            );
        }
        return op;
    }

    @Override
    public final PhysicalOperation sourcePhysicalOperation(EsQueryExec esQueryExec, LocalExecutionPlannerContext context) {

        LuceneOperator.LuceneOperatorFactory operatorFactory = null;
        Function<SearchContext, Query> querySupplier = searchContext -> {
            SearchExecutionContext ctx = searchContext.getSearchExecutionContext();
            Query query = ctx.toQuery(esQueryExec.query()).query();
            NestedLookup nestedLookup = ctx.nestedLookup();
            if (nestedLookup != NestedLookup.EMPTY) {
                NestedHelper nestedHelper = new NestedHelper(nestedLookup, ctx::isFieldMapped);
                if (nestedHelper.mightMatchNestedDocs(query)) {
                    // filter out nested documents
                    query = new BooleanQuery.Builder().add(query, BooleanClause.Occur.MUST)
                        .add(newNonNestedFilter(ctx.indexVersionCreated()), BooleanClause.Occur.FILTER)
                        .build();
                }
            }
            AliasFilter aliasFilter = searchContext.request().getAliasFilter();
            if (aliasFilter != AliasFilter.EMPTY) {
                Query filterQuery = ctx.toQuery(aliasFilter.getQueryBuilder()).query();
                query = new BooleanQuery.Builder().add(query, BooleanClause.Occur.MUST)
                    .add(filterQuery, BooleanClause.Occur.FILTER)
                    .build();
            }
            return query;
        };

        List<FieldSort> sorts = esQueryExec.sorts();
        List<SortBuilder<?>> fieldSorts = null;
        if (sorts != null && sorts.isEmpty() == false) {
            fieldSorts = new ArrayList<>(sorts.size());
            for (FieldSort sort : sorts) {
                fieldSorts.add(sort.fieldSortBuilder());
            }
            operatorFactory = new LuceneTopNSourceOperatorFactory(
                searchContexts,
                querySupplier,
                context.dataPartitioning(),
                context.taskConcurrency(),
                context.pageSize(),
                esQueryExec.limit() != null ? (Integer) esQueryExec.limit().fold() : NO_LIMIT,
                fieldSorts
            );
        } else {
            operatorFactory = new LuceneSourceOperatorFactory(
                searchContexts,
                querySupplier,
                context.dataPartitioning(),
                context.taskConcurrency(),
                context.pageSize(),
                esQueryExec.limit() != null ? (Integer) esQueryExec.limit().fold() : NO_LIMIT
            );
        }
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
    public final Operator.OperatorFactory ordinalGroupingOperatorFactory(
        LocalExecutionPlanner.PhysicalOperation source,
        AggregateExec aggregateExec,
        List<GroupingAggregator.Factory> aggregatorFactories,
        Attribute attrSource,
        ElementType groupElementType,
        LocalExecutionPlannerContext context
    ) {
        var sourceAttribute = FieldExtractExec.extractSourceAttributesFrom(aggregateExec.child());
        int docChannel = source.layout.getChannel(sourceAttribute.id());
        // The grouping-by values are ready, let's group on them directly.
        // Costin: why are they ready and not already exposed in the layout?
        return new OrdinalsGroupingOperator.OrdinalsGroupingOperatorFactory(
            ValueSources.sources(
                searchContexts,
                attrSource.name(),
                EsqlDataTypes.isUnsupported(attrSource.dataType()),
                LocalExecutionPlanner.toElementType(attrSource.dataType())
            ),
            docChannel,
            attrSource.name(),
            aggregatorFactories,
            context.pageSize(),
            context.bigArrays()
        );
    }
}
