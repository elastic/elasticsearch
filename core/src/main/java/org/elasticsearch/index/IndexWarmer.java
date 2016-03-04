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

import com.carrotsearch.hppc.ObjectHashSet;
import com.carrotsearch.hppc.ObjectSet;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.FieldDataType;
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

/**
 */
public final class IndexWarmer extends AbstractComponent {

    public static final Setting<MappedFieldType.Loading> INDEX_NORMS_LOADING_SETTING = new Setting<>("index.norms.loading",
        MappedFieldType.Loading.LAZY.toString(), (s) -> MappedFieldType.Loading.parse(s, MappedFieldType.Loading.LAZY),
        Property.IndexScope);
    private final List<Listener> listeners;

    IndexWarmer(Settings settings, ThreadPool threadPool, Listener... listeners) {
        super(settings);
        ArrayList<Listener> list = new ArrayList<>();
        final Executor executor = threadPool.executor(ThreadPool.Names.WARMER);
        list.add(new NormsWarmer(executor));
        list.add(new FieldDataWarmer(executor));
        for (Listener listener : listeners) {
            list.add(listener);
        }
        this.listeners = Collections.unmodifiableList(list);
    }

    void warm(Engine.Searcher searcher, IndexShard shard, IndexSettings settings, boolean isTopReader) {
        if (shard.state() == IndexShardState.CLOSED) {
            return;
        }
        if (settings.isWarmerEnabled() == false) {
            return;
        }
        if (logger.isTraceEnabled()) {
            if (isTopReader) {
                logger.trace("{} top warming [{}]", shard.shardId(), searcher.reader());
            } else {
                logger.trace("{} warming [{}]", shard.shardId(), searcher.reader());
            }
        }
        shard.warmerService().onPreWarm();
        long time = System.nanoTime();
        final List<TerminationHandle> terminationHandles = new ArrayList<>();
        // get a handle on pending tasks
        for (final Listener listener : listeners) {
            if (isTopReader) {
                terminationHandles.add(listener.warmTopReader(shard, searcher));
            } else {
                terminationHandles.add(listener.warmNewReaders(shard, searcher));
            }
        }
        // wait for termination
        for (TerminationHandle terminationHandle : terminationHandles) {
            try {
                terminationHandle.awaitTermination();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                if (isTopReader) {
                    logger.warn("top warming has been interrupted", e);
                } else {
                    logger.warn("warming has been interrupted", e);
                }
                break;
            }
        }
        long took = System.nanoTime() - time;
        shard.warmerService().onPostWarm(took);
        if (shard.warmerService().logger().isTraceEnabled()) {
            if (isTopReader) {
                shard.warmerService().logger().trace("top warming took [{}]", new TimeValue(took, TimeUnit.NANOSECONDS));
            } else {
                shard.warmerService().logger().trace("warming took [{}]", new TimeValue(took, TimeUnit.NANOSECONDS));
            }
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
        TerminationHandle warmNewReaders(IndexShard indexShard, Engine.Searcher searcher);

        TerminationHandle warmTopReader(IndexShard indexShard, Engine.Searcher searcher);
    }

    private static class NormsWarmer implements IndexWarmer.Listener {
        private final Executor executor;
        public NormsWarmer(Executor executor) {
            this.executor = executor;
        }
        @Override
        public TerminationHandle warmNewReaders(final IndexShard indexShard, final Engine.Searcher searcher) {
            final MappedFieldType.Loading defaultLoading = indexShard.indexSettings().getValue(INDEX_NORMS_LOADING_SETTING);
            final MapperService mapperService = indexShard.mapperService();
            final ObjectSet<String> warmUp = new ObjectHashSet<>();
            for (DocumentMapper docMapper : mapperService.docMappers(false)) {
                for (FieldMapper fieldMapper : docMapper.mappers()) {
                    final String indexName = fieldMapper.fieldType().name();
                    MappedFieldType.Loading normsLoading = fieldMapper.fieldType().normsLoading();
                    if (normsLoading == null) {
                        normsLoading = defaultLoading;
                    }
                    if (fieldMapper.fieldType().indexOptions() != IndexOptions.NONE && !fieldMapper.fieldType().omitNorms()
                        && normsLoading == MappedFieldType.Loading.EAGER) {
                        warmUp.add(indexName);
                    }
                }
            }

            final CountDownLatch latch = new CountDownLatch(1);
            // Norms loading may be I/O intensive but is not CPU intensive, so we execute it in a single task
            executor.execute(() -> {
                try {
                    for (ObjectCursor<String> stringObjectCursor : warmUp) {
                        final String indexName = stringObjectCursor.value;
                        final long start = System.nanoTime();
                        for (final LeafReaderContext ctx : searcher.reader().leaves()) {
                            final NumericDocValues values = ctx.reader().getNormValues(indexName);
                            if (values != null) {
                                values.get(0);
                            }
                        }
                        if (indexShard.warmerService().logger().isTraceEnabled()) {
                            indexShard.warmerService().logger().trace("warmed norms for [{}], took [{}]", indexName,
                                TimeValue.timeValueNanos(System.nanoTime() - start));
                        }
                    }
                } catch (Throwable t) {
                    indexShard.warmerService().logger().warn("failed to warm-up norms", t);
                } finally {
                    latch.countDown();
                }
            });

            return () -> latch.await();
        }

        @Override
        public TerminationHandle warmTopReader(IndexShard indexShard, final Engine.Searcher searcher) {
            return TerminationHandle.NO_WAIT;
        }
    }

    private static class FieldDataWarmer implements IndexWarmer.Listener {

        private final Executor executor;
        public FieldDataWarmer(Executor executor) {
            this.executor = executor;
        }

        @Override
        public TerminationHandle warmNewReaders(final IndexShard indexShard, final Engine.Searcher searcher) {
            final MapperService mapperService = indexShard.mapperService();
            final Map<String, MappedFieldType> warmUp = new HashMap<>();
            for (DocumentMapper docMapper : mapperService.docMappers(false)) {
                for (FieldMapper fieldMapper : docMapper.mappers()) {
                    final FieldDataType fieldDataType = fieldMapper.fieldType().fieldDataType();
                    final String indexName = fieldMapper.fieldType().name();
                    if (fieldDataType == null) {
                        continue;
                    }
                    if (fieldDataType.getLoading() == MappedFieldType.Loading.LAZY) {
                        continue;
                    }

                    if (warmUp.containsKey(indexName)) {
                        continue;
                    }
                    warmUp.put(indexName, fieldMapper.fieldType());
                }
            }
            final IndexFieldDataService indexFieldDataService = indexShard.indexFieldDataService();
            final CountDownLatch latch = new CountDownLatch(searcher.reader().leaves().size() * warmUp.size());
            for (final LeafReaderContext ctx : searcher.reader().leaves()) {
                for (final MappedFieldType fieldType : warmUp.values()) {
                    executor.execute(() -> {
                        try {
                            final long start = System.nanoTime();
                            indexFieldDataService.getForField(fieldType).load(ctx);
                            if (indexShard.warmerService().logger().isTraceEnabled()) {
                                indexShard.warmerService().logger().trace("warmed fielddata for [{}], took [{}]", fieldType.name(),
                                    TimeValue.timeValueNanos(System.nanoTime() - start));
                            }
                        } catch (Throwable t) {
                            indexShard.warmerService().logger().warn("failed to warm-up fielddata for [{}]", t, fieldType.name());
                        } finally {
                            latch.countDown();
                        }
                    });
                }
            }
            return () -> latch.await();
        }

        @Override
        public TerminationHandle warmTopReader(final IndexShard indexShard, final Engine.Searcher searcher) {
            final MapperService mapperService = indexShard.mapperService();
            final Map<String, MappedFieldType> warmUpGlobalOrdinals = new HashMap<>();
            for (DocumentMapper docMapper : mapperService.docMappers(false)) {
                for (FieldMapper fieldMapper : docMapper.mappers()) {
                    final FieldDataType fieldDataType = fieldMapper.fieldType().fieldDataType();
                    final String indexName = fieldMapper.fieldType().name();
                    if (fieldDataType == null) {
                        continue;
                    }
                    if (fieldDataType.getLoading() != MappedFieldType.Loading.EAGER_GLOBAL_ORDINALS) {
                        continue;
                    }
                    if (warmUpGlobalOrdinals.containsKey(indexName)) {
                        continue;
                    }
                    warmUpGlobalOrdinals.put(indexName, fieldMapper.fieldType());
                }
            }
            final IndexFieldDataService indexFieldDataService = indexShard.indexFieldDataService();
            final CountDownLatch latch = new CountDownLatch(warmUpGlobalOrdinals.size());
            for (final MappedFieldType fieldType : warmUpGlobalOrdinals.values()) {
                executor.execute(() -> {
                    try {
                        final long start = System.nanoTime();
                        IndexFieldData.Global ifd = indexFieldDataService.getForField(fieldType);
                        ifd.loadGlobal(searcher.getDirectoryReader());
                        if (indexShard.warmerService().logger().isTraceEnabled()) {
                            indexShard.warmerService().logger().trace("warmed global ordinals for [{}], took [{}]", fieldType.name(),
                                TimeValue.timeValueNanos(System.nanoTime() - start));
                        }
                    } catch (Throwable t) {
                        indexShard.warmerService().logger().warn("failed to warm-up global ordinals for [{}]", t, fieldType.name());
                    } finally {
                        latch.countDown();
                    }
                });
            }
            return () -> latch.await();
        }
    }

}
