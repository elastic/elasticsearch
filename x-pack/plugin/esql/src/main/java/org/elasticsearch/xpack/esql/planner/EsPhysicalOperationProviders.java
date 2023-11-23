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
import org.elasticsearch.compute.lucene.BlockReaderFactories;
import org.elasticsearch.compute.lucene.LuceneOperator;
import org.elasticsearch.compute.lucene.LuceneSourceOperator;
import org.elasticsearch.compute.lucene.LuceneTopNSourceOperator;
import org.elasticsearch.compute.lucene.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.OrdinalsGroupingOperator;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.NestedLookup;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
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
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.type.DataType;

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

    public List<SearchContext> searchContexts() {
        return searchContexts;
    }

    @Override
    public final PhysicalOperation fieldExtractPhysicalOperation(FieldExtractExec fieldExtractExec, PhysicalOperation source) {
        Layout.Builder layout = source.layout.builder();
        var sourceAttr = fieldExtractExec.sourceAttribute();
        List<ValuesSourceReaderOperator.ShardContext> readers = searchContexts.stream()
            .map(s -> new ValuesSourceReaderOperator.ShardContext(s.searcher().getIndexReader(), s::newSourceLoader))
            .toList();
        List<ValuesSourceReaderOperator.FieldInfo> fields = new ArrayList<>();
        int docChannel = source.layout.get(sourceAttr.id()).channel();
        for (Attribute attr : fieldExtractExec.attributesToExtract()) {
            if (attr instanceof FieldAttribute fa && fa.getExactInfo().hasExact()) {
                attr = fa.exactAttribute();
            }
            layout.append(attr);
            DataType dataType = attr.dataType();
            String fieldName = attr.name();
            List<BlockLoader> loaders = BlockReaderFactories.loaders(searchContexts, fieldName, EsqlDataTypes.isUnsupported(dataType));
            fields.add(new ValuesSourceReaderOperator.FieldInfo(fieldName, loaders));
        }
        return source.with(new ValuesSourceReaderOperator.Factory(fields, readers, docChannel), layout.build());
    }

    public static Function<SearchContext, Query> querySupplier(QueryBuilder queryBuilder) {
        final QueryBuilder qb = queryBuilder == null ? QueryBuilders.matchAllQuery() : queryBuilder;

        return searchContext -> {
            SearchExecutionContext ctx = searchContext.getSearchExecutionContext();
            Query query = ctx.toQuery(qb).query();
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
    }

    @Override
    public final PhysicalOperation sourcePhysicalOperation(EsQueryExec esQueryExec, LocalExecutionPlannerContext context) {
        Function<SearchContext, Query> querySupplier = querySupplier(esQueryExec.query());
        final LuceneOperator.Factory luceneFactory;

        List<FieldSort> sorts = esQueryExec.sorts();
        List<SortBuilder<?>> fieldSorts = null;
        assert esQueryExec.estimatedRowSize() != null : "estimated row size not initialized";
        int rowEstimatedSize = esQueryExec.estimatedRowSize();
        int limit = esQueryExec.limit() != null ? (Integer) esQueryExec.limit().fold() : NO_LIMIT;
        if (sorts != null && sorts.isEmpty() == false) {
            fieldSorts = new ArrayList<>(sorts.size());
            for (FieldSort sort : sorts) {
                fieldSorts.add(sort.fieldSortBuilder());
            }
            luceneFactory = new LuceneTopNSourceOperator.Factory(
                searchContexts,
                querySupplier,
                context.queryPragmas().dataPartitioning(),
                context.queryPragmas().taskConcurrency(),
                context.pageSize(rowEstimatedSize),
                limit,
                fieldSorts
            );
        } else {
            luceneFactory = new LuceneSourceOperator.Factory(
                searchContexts,
                querySupplier,
                context.queryPragmas().dataPartitioning(),
                context.queryPragmas().taskConcurrency(),
                context.pageSize(rowEstimatedSize),
                limit
            );
        }
        Layout.Builder layout = new Layout.Builder();
        layout.append(esQueryExec.output());
        int instanceCount = Math.max(1, luceneFactory.taskConcurrency());
        context.driverParallelism(new DriverParallelism(DriverParallelism.Type.DATA_PARALLELISM, instanceCount));
        return PhysicalOperation.fromSource(luceneFactory, layout.build());
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
        int docChannel = source.layout.get(sourceAttribute.id()).channel();
        List<ValuesSourceReaderOperator.ShardContext> shardContexts = searchContexts.stream()
            .map(s -> new ValuesSourceReaderOperator.ShardContext(s.searcher().getIndexReader(), s::newSourceLoader))
            .toList();
        // The grouping-by values are ready, let's group on them directly.
        // Costin: why are they ready and not already exposed in the layout?
        return new OrdinalsGroupingOperator.OrdinalsGroupingOperatorFactory(
            BlockReaderFactories.loaders(searchContexts, attrSource.name(), EsqlDataTypes.isUnsupported(attrSource.dataType())),
            shardContexts,
            groupElementType,
            docChannel,
            attrSource.name(),
            aggregatorFactories,
            context.pageSize(aggregateExec.estimatedRowSize()),
            context.bigArrays()
        );
    }
}
