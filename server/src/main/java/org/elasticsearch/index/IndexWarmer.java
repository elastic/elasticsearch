/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

public final class IndexWarmer {

    private static final Logger logger = LogManager.getLogger(IndexWarmer.class);

    private final List<Listener> listeners;

    IndexWarmer(ThreadPool threadPool, IndexFieldDataService indexFieldDataService,
                Listener... listeners) {
        ArrayList<Listener> list = new ArrayList<>();
        final Executor executor = threadPool.executor(ThreadPool.Names.WARMER);
        list.add(new FieldDataWarmer(executor, indexFieldDataService));

        Collections.addAll(list, listeners);
        this.listeners = Collections.unmodifiableList(list);
    }

    void warm(ElasticsearchDirectoryReader reader, IndexShard shard, IndexSettings settings) {
        if (shard.state() == IndexShardState.CLOSED) {
            return;
        }
        if (settings.isWarmerEnabled() == false) {
            return;
        }
        if (logger.isTraceEnabled()) {
            logger.trace("{} top warming [{}]", shard.shardId(), reader);
        }
        shard.warmerService().onPreWarm();
        long time = System.nanoTime();
        final List<TerminationHandle> terminationHandles = new ArrayList<>();
        // get a handle on pending tasks
        for (final Listener listener : listeners) {
            terminationHandles.add(listener.warmReader(shard, reader));
        }
        // wait for termination
        for (TerminationHandle terminationHandle : terminationHandles) {
            try {
                terminationHandle.awaitTermination();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("top warming has been interrupted", e);
                break;
            }
        }
        long took = System.nanoTime() - time;
        shard.warmerService().onPostWarm(took);
        if (shard.warmerService().logger().isTraceEnabled()) {
            shard.warmerService().logger().trace("top warming took [{}]", new TimeValue(took, TimeUnit.NANOSECONDS));
        }
    }

    /** A handle on the execution of  warm-up action. */
    public interface TerminationHandle {

        TerminationHandle NO_WAIT = () -> {};

        /** Wait until execution of the warm-up action completes. */
        void awaitTermination() throws InterruptedException;
    }
    public interface Listener {
        /** Queue tasks to warm-up the given segments and return handles that allow to wait for termination of the
         *  execution of those tasks. */
        TerminationHandle warmReader(IndexShard indexShard, ElasticsearchDirectoryReader reader);
    }

    private static class FieldDataWarmer implements IndexWarmer.Listener {

        private final Executor executor;
        private final IndexFieldDataService indexFieldDataService;

        FieldDataWarmer(Executor executor, IndexFieldDataService indexFieldDataService) {
            this.executor = executor;
            this.indexFieldDataService = indexFieldDataService;
        }

        @Override
        public TerminationHandle warmReader(final IndexShard indexShard, final ElasticsearchDirectoryReader reader) {
            final MapperService mapperService = indexShard.mapperService();
            final Map<String, MappedFieldType> warmUpGlobalOrdinals = new HashMap<>();
            for (MappedFieldType fieldType : mapperService.getEagerGlobalOrdinalsFields()) {
                final String indexName = fieldType.name();
                warmUpGlobalOrdinals.put(indexName, fieldType);
            }
            final CountDownLatch latch = new CountDownLatch(warmUpGlobalOrdinals.size());
            for (final MappedFieldType fieldType : warmUpGlobalOrdinals.values()) {
                executor.execute(() -> {
                    try {
                        final long start = System.nanoTime();
                        IndexFieldData.Global<?> ifd = indexFieldDataService.getForField(fieldType, indexFieldDataService.index().getName(),
                            () -> {
                                throw new UnsupportedOperationException("search lookup not available when warming an index");
                            });
                        IndexFieldData<?> global = ifd.loadGlobal(reader);
                        if (reader.leaves().isEmpty() == false) {
                            global.load(reader.leaves().get(0));
                        }

                        if (indexShard.warmerService().logger().isTraceEnabled()) {
                            indexShard.warmerService().logger().trace(
                                "warmed global ordinals for [{}], took [{}]",
                                fieldType.name(),
                                TimeValue.timeValueNanos(System.nanoTime() - start));
                        }
                    } catch (Exception e) {
                        indexShard
                            .warmerService()
                            .logger()
                            .warn(() -> new ParameterizedMessage("failed to warm-up global ordinals for [{}]", fieldType.name()), e);
                    } finally {
                        latch.countDown();
                    }
                });
            }
            return () -> latch.await();
        }
    }

}
