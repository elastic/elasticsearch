/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.apache.lucene.index.DirectoryReader;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
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

public final class IndexWarmer extends AbstractComponent {

    private final List<Listener> listeners;

    IndexWarmer(Settings settings, ThreadPool threadPool, IndexFieldDataService indexFieldDataService,
                Listener... listeners) {
        super(settings);
        ArrayList<Listener> list = new ArrayList<>();
        final Executor executor = threadPool.executor(ThreadPool.Names.WARMER);
        list.add(new FieldDataWarmer(executor, indexFieldDataService));

        Collections.addAll(list, listeners);
        this.listeners = Collections.unmodifiableList(list);
    }

    void warm(Engine.Searcher searcher, IndexShard shard, IndexSettings settings) {
        if (shard.state() == IndexShardState.CLOSED) {
            return;
        }
        if (settings.isWarmerEnabled() == false) {
            return;
        }
        if (logger.isTraceEnabled()) {
            logger.trace("{} top warming [{}]", shard.shardId(), searcher.reader());
        }
        shard.warmerService().onPreWarm();
        long time = System.nanoTime();
        final List<TerminationHandle> terminationHandles = new ArrayList<>();
        // get a handle on pending tasks
        for (final Listener listener : listeners) {
            terminationHandles.add(listener.warmReader(shard, searcher));
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
        TerminationHandle warmReader(IndexShard indexShard, Engine.Searcher searcher);
    }

    private static class FieldDataWarmer implements IndexWarmer.Listener {

        private final Executor executor;
        private final IndexFieldDataService indexFieldDataService;

        FieldDataWarmer(Executor executor, IndexFieldDataService indexFieldDataService) {
            this.executor = executor;
            this.indexFieldDataService = indexFieldDataService;
        }

        @Override
        public TerminationHandle warmReader(final IndexShard indexShard, final Engine.Searcher searcher) {
            final MapperService mapperService = indexShard.mapperService();
            final Map<String, MappedFieldType> warmUpGlobalOrdinals = new HashMap<>();
            for (DocumentMapper docMapper : mapperService.docMappers(false)) {
                for (FieldMapper fieldMapper : docMapper.mappers()) {
                    final MappedFieldType fieldType = fieldMapper.fieldType();
                    final String indexName = fieldType.name();
                    if (fieldType.eagerGlobalOrdinals() == false) {
                        continue;
                    }
                    warmUpGlobalOrdinals.put(indexName, fieldType);
                }
            }
            final CountDownLatch latch = new CountDownLatch(warmUpGlobalOrdinals.size());
            for (final MappedFieldType fieldType : warmUpGlobalOrdinals.values()) {
                executor.execute(() -> {
                    try {
                        final long start = System.nanoTime();
                        IndexFieldData.Global ifd = indexFieldDataService.getForField(fieldType);
                        DirectoryReader reader = searcher.getDirectoryReader();
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
                            .warn(
                                (Supplier<?>) () -> new ParameterizedMessage(
                                    "failed to warm-up global ordinals for [{}]", fieldType.name()), e);
                    } finally {
                        latch.countDown();
                    }
                });
            }
            return () -> latch.await();
        }
    }

}
