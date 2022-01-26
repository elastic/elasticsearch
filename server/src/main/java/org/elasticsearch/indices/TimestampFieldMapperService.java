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
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;

/**
 * Tracks the mapping of the {@code @timestamp} field of immutable indices that expose their timestamp range in their index metadata.
 * Coordinating nodes do not have (easy) access to mappings for all indices, so we extract the type of this one field from the mapping here.
 */
public class TimestampFieldMapperService extends AbstractLifecycleComponent implements ClusterStateApplier {

    private static final Logger logger = LogManager.getLogger(TimestampFieldMapperService.class);

    private final IndicesService indicesService;
    private final ExecutorService executor; // single thread to construct mapper services async as needed

    /**
     * The type of the {@code @timestamp} field keyed by index. Futures may be completed with {@code null} to indicate that there is
     * no usable {@code @timestamp} field.
     */
    private final Map<Index, PlainActionFuture<DateFieldMapper.DateFieldType>> fieldTypesByIndex = ConcurrentCollections.newConcurrentMap();

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
        final ImmutableOpenMap<String, IndexMetadata> indices = metadata.indices();
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
                final PlainActionFuture<DateFieldMapper.DateFieldType> future = new PlainActionFuture<>();
                fieldTypesByIndex.put(index, future);

                final IndexService indexService = indicesService.indexService(index);
                if (indexService == null) {
                    logger.trace("computing timestamp mapping for {} async", index);
                    executor.execute(new AbstractRunnable() {
                        @Override
                        public void onFailure(Exception e) {
                            logger.debug(new ParameterizedMessage("failed to compute mapping for {}", index), e);
                            future.onResponse(null); // no need to propagate a failure to create the mapper service to searches
                        }

                        @Override
                        protected void doRun() throws Exception {
                            try (MapperService mapperService = indicesService.createIndexMapperService(indexMetadata)) {
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
        final IndexLongFieldRange timestampRange = indexMetadata.getTimestampRange();
        return timestampRange.isComplete() && timestampRange != IndexLongFieldRange.UNKNOWN;
    }

    private static DateFieldMapper.DateFieldType fromMapperService(MapperService mapperService) {
        final MappedFieldType mappedFieldType = mapperService.fieldType(DataStream.TimestampField.FIXED_TIMESTAMP_FIELD);
        if (mappedFieldType instanceof DateFieldMapper.DateFieldType) {
            return (DateFieldMapper.DateFieldType) mappedFieldType;
        } else {
            return null;
        }
    }

    /**
     * @return the field type of the {@code @timestamp} field of the given index, or {@code null} if:
     * - the index is not found,
     * - the field is not found,
     * - the mapping is not known yet, or
     * - the field is not a timestamp field.
     */
    @Nullable
    public DateFieldMapper.DateFieldType getTimestampFieldType(Index index) {
        final PlainActionFuture<DateFieldMapper.DateFieldType> future = fieldTypesByIndex.get(index);
        if (future == null || future.isDone() == false) {
            return null;
        }

        try {
            // It's possible that callers of this class are executed
            // in a transport thread, for that reason we request
            // the future value with a timeout of 0. That won't
            // trigger assertion errors.
            return future.actionGet(TimeValue.ZERO);
        } catch (ElasticsearchTimeoutException e) {
            assert false : "Unexpected timeout exception while getting a timestamp mapping";
            throw e;
        }
    }

}
