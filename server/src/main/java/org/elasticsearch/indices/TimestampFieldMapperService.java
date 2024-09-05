/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.UncategorizedExecutionException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.shard.IndexLongFieldRange;
import org.elasticsearch.node.Node;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;
import static org.elasticsearch.core.Strings.format;

/**
 * Tracks the mapping of the '@timestamp' and 'event.ingested' fields of immutable indices that expose their timestamp range in their
 * index metadata. Coordinating nodes do not have (easy) access to mappings for all indices, so we extract the type of these two fields
 * from the mapping here, since timestamp fields can have millis or nanos level resolution.
 */
public class TimestampFieldMapperService extends AbstractLifecycleComponent implements ClusterStateApplier {

    private static final Logger logger = LogManager.getLogger(TimestampFieldMapperService.class);

    private final IndicesService indicesService;
    private final ExecutorService executor; // single thread to construct mapper services async as needed

    /**
     * The type of the 'event.ingested' and/or '@timestamp' fields keyed by index.
     * The inner map is keyed by field name ('@timestamp' or 'event.ingested').
     * Futures may be completed with {@code null} to indicate that there is
     * no usable timestamp field.
     */
    private final Map<Index, PlainActionFuture<DateFieldRangeInfo>> fieldTypesByIndex = ConcurrentCollections.newConcurrentMap();

    public TimestampFieldMapperService(Settings settings, ThreadPool threadPool, IndicesService indicesService) {
        this.indicesService = indicesService;

        final String nodeName = Objects.requireNonNull(Node.NODE_NAME_SETTING.get(settings));
        final String threadName = "TimestampFieldMapperService#updateTask";
        executor = EsExecutors.newScaling(
            nodeName + "/" + threadName,
            0,
            1,
            0,
            TimeUnit.MILLISECONDS,
            true,
            daemonThreadFactory(nodeName, threadName),
            threadPool.getThreadContext()
        );
    }

    @Override
    protected void doStart() {}

    @Override
    protected void doStop() {
        ThreadPool.terminate(executor, 10, TimeUnit.SECONDS);
    }

    @Override
    protected void doClose() {}

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        final Metadata metadata = event.state().metadata();
        final Map<String, IndexMetadata> indices = metadata.indices();
        if (indices == event.previousState().metadata().indices()) {
            return;
        }

        // clear out mappers for indices that no longer exist or whose timestamp range is no longer known
        fieldTypesByIndex.keySet().removeIf(index -> hasUsefulTimestampField(metadata.index(index)) == false);

        // capture mappers for indices that do exist
        for (IndexMetadata indexMetadata : indices.values()) {
            final Index index = indexMetadata.getIndex();

            if (hasUsefulTimestampField(indexMetadata) && fieldTypesByIndex.containsKey(index) == false) {
                logger.trace("computing timestamp mapping for {}", index);
                final PlainActionFuture<DateFieldRangeInfo> future = new PlainActionFuture<>();
                fieldTypesByIndex.put(index, future);

                final IndexService indexService = indicesService.indexService(index);
                if (indexService == null) {
                    logger.trace("computing timestamp mapping for {} async", index);
                    executor.execute(new AbstractRunnable() {
                        @Override
                        public void onFailure(Exception e) {
                            logger.debug(() -> format("failed to compute mapping for %s", index), e);
                            future.onResponse(null); // no need to propagate a failure to create the mapper service to searches
                        }

                        @Override
                        protected void doRun() throws Exception {
                            try (MapperService mapperService = indicesService.createIndexMapperServiceForValidation(indexMetadata)) {
                                mapperService.merge(indexMetadata, MapperService.MergeReason.MAPPING_RECOVERY);
                                logger.trace("computed timestamp field mapping for {}", index);
                                future.onResponse(fromMapperService(mapperService));
                            }
                        }
                    });
                } else {
                    logger.trace("computing timestamp mapping for {} using existing index service", index);
                    try {
                        future.onResponse(fromMapperService(indexService.mapperService()));
                    } catch (Exception e) {
                        assert false : e;
                        future.onResponse(null);
                    }
                }
            }
        }
    }

    private static boolean hasUsefulTimestampField(IndexMetadata indexMetadata) {
        if (indexMetadata == null) {
            return false;
        }

        if (indexMetadata.hasTimeSeriesTimestampRange()) {
            // Tsdb indices have @timestamp field and index.time_series.start_time / index.time_series.end_time range
            return true;
        }

        IndexLongFieldRange timestampRange = indexMetadata.getTimestampRange();
        if (timestampRange.isComplete() && timestampRange != IndexLongFieldRange.UNKNOWN) {
            return true;
        }

        IndexLongFieldRange eventIngestedRange = indexMetadata.getEventIngestedRange();
        return eventIngestedRange.isComplete() && eventIngestedRange != IndexLongFieldRange.UNKNOWN;
    }

    private static DateFieldRangeInfo fromMapperService(MapperService mapperService) {
        DateFieldMapper.DateFieldType timestampFieldType = null;
        DateFieldMapper.DateFieldType eventIngestedFieldType = null;

        MappedFieldType mappedFieldType = mapperService.fieldType(DataStream.TIMESTAMP_FIELD_NAME);
        if (mappedFieldType instanceof DateFieldMapper.DateFieldType dateFieldType) {
            timestampFieldType = dateFieldType;
        }
        mappedFieldType = mapperService.fieldType(IndexMetadata.EVENT_INGESTED_FIELD_NAME);
        if (mappedFieldType instanceof DateFieldMapper.DateFieldType dateFieldType) {
            eventIngestedFieldType = dateFieldType;
        }
        if (timestampFieldType == null && eventIngestedFieldType == null) {
            return null;
        }
        // the mapper only fills in the field types, not the actual range values
        return new DateFieldRangeInfo(timestampFieldType, null, eventIngestedFieldType, null);
    }

    /**
     * @return DateFieldRangeInfo holding the field types of the {@code @timestamp} and {@code event.ingested} fields of the index.
     * or {@code null} if:
     * - the index is not found,
     * - the field is not found,
     * - the mapping is not known yet, or
     * - the index does not have a useful timestamp field.
     */
    @Nullable
    public DateFieldRangeInfo getTimestampFieldTypeInfo(Index index) {
        final PlainActionFuture<DateFieldRangeInfo> future = fieldTypesByIndex.get(index);
        if (future == null || future.isDone() == false) {
            return null;
        }
        // call non-blocking result() as we could be on a network or scheduler thread which we must not block
        try {
            return future.result();
        } catch (ExecutionException e) {
            throw new UncategorizedExecutionException("An error occurred fetching timestamp field type for " + index, e);
        }
    }
}
