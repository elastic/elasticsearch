/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Loads concrete field values for remote fetch handles using retained shard contexts.
 */
final class RemoteFieldLoader {
    private final BigArrays bigArrays;
    private final LocalCircuitBreaker.SizeSettings localBreakerSettings;

    RemoteFieldLoader(BigArrays bigArrays, LocalCircuitBreaker.SizeSettings localBreakerSettings) {
        this.bigArrays = bigArrays;
        this.localBreakerSettings = localBreakerSettings;
    }

    List<Page> execute(
        List<RemoteFetchHandle> handles,
        List<RemoteFetchService.FetchField> fields,
        IndexedByShardId<? extends EsPhysicalOperationProviders.ShardContext> shardContexts,
        PlannerSettings settings,
        BlockFactory executionBlockFactory
    ) {
        List<ValuesSourceReaderOperator.FieldInfo> fieldInfos = buildFieldInfos(fields, shardContexts, settings);
        IndexedByShardId<ValuesSourceReaderOperator.ShardContext> readerContexts = shardContexts.map(
            c -> new ValuesSourceReaderOperator.ShardContext(
                c.searcher().getIndexReader(),
                c::newSourceLoader,
                c.storedFieldsSequentialProportion()
            )
        );
        return executeFetch(handles, fieldInfos, readerContexts, bigArrays, executionBlockFactory, localBreakerSettings, settings);
    }

    static List<ValuesSourceReaderOperator.FieldInfo> buildFieldInfos(
        List<RemoteFetchService.FetchField> fields,
        IndexedByShardId<? extends EsPhysicalOperationProviders.ShardContext> shardContexts,
        PlannerSettings plannerSettings
    ) {
        List<ValuesSourceReaderOperator.FieldInfo> fieldInfos = new ArrayList<>(fields.size());
        for (RemoteFetchService.FetchField field : fields) {
            fieldInfos.add(
                new ValuesSourceReaderOperator.FieldInfo(
                    field.fieldName(),
                    PlannerUtils.toElementType(field.dataType()),
                    false,
                    (warningsMode, shardIdx) -> {
                        BlockLoader loader = shardContexts.get(shardIdx)
                            .blockLoader(
                                field.fieldName(),
                                field.dataType() == DataType.UNSUPPORTED,
                                MappedFieldType.FieldExtractPreference.NONE,
                                null,
                                null,
                                plannerSettings.blockLoaderSizeOrdinals(),
                                plannerSettings.blockLoaderSizeScript()
                            );
                        return ValuesSourceReaderOperator.load(loader);
                    }
                )
            );
        }
        return fieldInfos;
    }

    static List<Page> executeFetch(
        List<RemoteFetchHandle> handles,
        List<ValuesSourceReaderOperator.FieldInfo> fieldInfos,
        IndexedByShardId<ValuesSourceReaderOperator.ShardContext> shardContexts,
        BigArrays bigArrays,
        BlockFactory blockFactory,
        LocalCircuitBreaker.SizeSettings localBreakerSettings,
        PlannerSettings plannerSettings
    ) {
        if (handles.isEmpty()) {
            return List.of();
        }
        if (fieldInfos.isEmpty()) {
            throw new IllegalArgumentException("remote fetch requires at least one field");
        }

        final LocalCircuitBreaker localBreaker = new LocalCircuitBreaker(
            blockFactory.breaker(),
            localBreakerSettings.overReservedBytes(),
            localBreakerSettings.maxOverReservedBytes()
        );
        final DriverContext driverContext = new DriverContext(
            bigArrays,
            blockFactory.newChildFactory(localBreaker),
            localBreakerSettings,
            "remote_fetch"
        );
        final Operator operator = new ValuesSourceReaderOperator.Factory(
            plannerSettings.valuesLoadingJumboSize(),
            fieldInfos,
            shardContexts,
            fieldInfos.size() <= plannerSettings.reuseColumnLoadersThreshold(),
            0,
            plannerSettings.sourceReservationFactor(),
            plannerSettings.docSequenceBytesRefFieldThreshold()
        ).get(driverContext);
        final int[] projection = new int[fieldInfos.size()];
        for (int i = 0; i < projection.length; i++) {
            projection[i] = i + 1;
        }
        Page inputPage = inputPage(driverContext.blockFactory(), handles);
        boolean releaseInputPage = true;
        boolean success = false;
        List<Page> outputPages = new ArrayList<>();
        try {
            operator.addInput(inputPage);
            releaseInputPage = false;
            operator.finish();
            while (operator.isFinished() == false) {
                Page page = operator.getOutput();
                if (page == null) {
                    throw new IllegalStateException("remote fetch operator stalled without producing output");
                }
                Page projected = page.projectBlocks(projection);
                page.releaseBlocks();
                projected.allowPassingToDifferentDriver();
                outputPages.add(projected);
            }
            success = true;
            return outputPages;
        } finally {
            Releasables.closeExpectNoException(operator, localBreaker);
            if (releaseInputPage) {
                inputPage.releaseBlocks();
            }
            if (success == false) {
                Releasables.closeExpectNoException(Releasables.wrap(Iterators.map(outputPages.iterator(), page -> page::releaseBlocks)));
            }
        }
    }

    static Page inputPage(BlockFactory blockFactory, List<RemoteFetchHandle> handles) {
        try (DocVector.FixedBuilder builder = DocVector.newFixedBuilder(blockFactory, handles.size())) {
            for (RemoteFetchHandle handle : handles) {
                builder.append(handle.shard(), handle.segment(), handle.doc());
            }
            return new Page(builder.build(DocVector.config()).asBlock());
        }
    }
}
