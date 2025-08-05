/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FilterCodecReader;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.OneMergeWrappingMergePolicy;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFieldVisitor;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.codec.FilterDocValuesProducer;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.StreamSupport;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class CheckAbortedMergesIT extends ESIntegTestCase {

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(BlockRunningMergesEngineTestPlugin.class);
        return plugins;
    }

    public void testAbortedMerges() throws Exception {
        internalCluster().startMasterOnlyNode();
        var nodeA = internalCluster().startDataOnlyNode();

        var pluginA = internalCluster().getInstance(PluginsService.class, nodeA)
            .filterPlugins(BlockRunningMergesEngineTestPlugin.class)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Plugin not found"));

        final boolean checkAbortedMerges = false;randomBoolean();
        pluginA.blockMerges();

        final var indexName = randomIdentifier();
        createIndex(
            indexName,
            indexSettings(1, 0).put(CheckAbortedDuringMergePolicy.ENABLE_CHECK_ABORTED_DURING_MERGE.getKey(), checkAbortedMerges)
                .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE)
                .build()
        );

        var indexServiceA = internalCluster().getInstance(IndicesService.class, nodeA).indexService(resolveIndex(indexName));
        assertThat(indexServiceA.hasShard(0), equalTo(true));

        indexDocs(indexName, 10);
        flush(indexName);

        while (true) {
            indexDocs(indexName, 10);
            flush(indexName);

            var mergesStats = client().admin().indices().prepareStats(indexName).clear().setMerge(true).get();
            if (mergesStats.getIndices().get(indexName).getPrimaries().getMerge().getCurrent() > 0) {
                break;
            }
        }

        var nodeB = internalCluster().startDataOnlyNode();
        ensureStableCluster(3);

        pluginA.waitForMergesBlocked();

        ClusterRerouteUtils.reroute(client(), new MoveAllocationCommand(indexName, 0, nodeA, nodeB, ProjectId.DEFAULT));
        ensureGreen(indexName);

        var indexServiceB = internalCluster().getInstance(IndicesService.class, nodeB).indexService(resolveIndex(indexName));
        assertBusy(() -> assertThat(indexServiceB.hasShard(0), equalTo(true)));
        assertBusy(() -> assertThat(indexServiceA.hasShard(0), equalTo(false)));
        if (randomBoolean()) {
            forceMerge();
        }

        assertThat(pluginA.mergedDocsCount.get(), equalTo(0L));
        assertThat(pluginA.mergedFieldsCount.get(), equalTo(0L));
        assertThat(pluginA.checkIntegrityCount.get(), equalTo(0L));

        pluginA.unblockMerges();

        var mergeMetrics = internalCluster().getDataNodeInstances(MergeMetrics.class);
        assertBusy(
            () -> assertThat(
                StreamSupport.stream(mergeMetrics.spliterator(), false)
                    .mapToLong(m -> m.getQueuedMergeSizeInBytes() + m.getRunningMergeSizeInBytes())
                    .sum(),
                equalTo(0L)
            )
        );

        assertBusy(() -> {
            if (checkAbortedMerges) {
                assertThat(pluginA.mergedDocsCount.get(), equalTo(0L));
                assertThat(pluginA.mergedFieldsCount.get(), equalTo(0L));
                // Only the first integrity check is completed, the following ones should have been aborted
                assertThat(pluginA.checkIntegrityCount.get(), equalTo(1L));
            } else {
                assertThat(pluginA.mergedDocsCount.get(), greaterThan(0L));
                assertThat(pluginA.mergedFieldsCount.get(), greaterThan(0L));
                assertThat(pluginA.checkIntegrityCount.get(), greaterThan(1L));
            }
        });
    }

    private static BulkResponse indexDocs(String indexName, int numDocs) {
        final var client = client();
        var bulkRequest = client.prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < numDocs; i++) {
            var indexRequest = client.prepareIndex(indexName)
                .setSource(Map.of("text", randomUnicodeOfCodepointLengthBetween(1, 25), "integer", randomIntBetween(0, 100)));
            bulkRequest.add(indexRequest);
        }
        var bulkResponse = bulkRequest.get();
        assertNoFailures(bulkResponse);
        return bulkResponse;
    }

    /**
     * An engine plugin that allows to block running merges.
     *
     * Note: merges are blocked before executing the first integrity check on stored fields of the first segment to be merged
     */
    public static class BlockRunningMergesEngineTestPlugin extends Plugin implements EnginePlugin {

        // Merges are not blocked by default
        private final AtomicBoolean blockMerges = new AtomicBoolean(false);

        // Number of checkIntegrity() method calls that have been executed
        private final AtomicLong checkIntegrityCount = new AtomicLong(0L);

        // Number of time a field has been accessed during merges
        private final AtomicLong mergedFieldsCount = new AtomicLong(0L);

        // Number of time a doc has been accessed during merges
        private final AtomicLong mergedDocsCount = new AtomicLong(0L);

        // Used to block merges from running immediately
        private final AtomicBoolean mergesStarted = new AtomicBoolean();
        private final CountDownLatch mergesStartedLatch = new CountDownLatch(1);
        private final CountDownLatch resumeMerges = new CountDownLatch(1);

        void blockMerges() {
            if (blockMerges.compareAndSet(false, true) == false) {
                throw new AssertionError("Merges already blocked");
            }
        }

        void waitForMergesBlocked() {
            safeAwait(mergesStartedLatch);
        }

        void unblockMerges() {
            if (blockMerges.compareAndSet(true, false) == false) {
                throw new AssertionError("Merges already unblocked");
            }
            resumeMerges.countDown();
        }

        @Override
        public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
            return Optional.of(
                config -> new InternalEngine(
                    new EngineConfig(
                        config.getShardId(),
                        config.getThreadPool(),
                        config.getThreadPoolMergeExecutorService(),
                        config.getIndexSettings(),
                        config.getWarmer(),
                        config.getStore(),
                        wrapMergePolicy(config.getMergePolicy()),
                        config.getAnalyzer(),
                        config.getSimilarity(),
                        config.getCodecProvider(),
                        config.getEventListener(),
                        config.getQueryCache(),
                        config.getQueryCachingPolicy(),
                        config.getTranslogConfig(),
                        config.getFlushMergesAfter(),
                        config.getExternalRefreshListener(),
                        config.getInternalRefreshListener(),
                        config.getIndexSort(),
                        config.getCircuitBreakerService(),
                        config.getGlobalCheckpointSupplier(),
                        config.retentionLeasesSupplier(),
                        config.getPrimaryTermSupplier(),
                        config.getSnapshotCommitSupplier(),
                        config.getLeafSorter(),
                        config.getRelativeTimeInNanosSupplier(),
                        config.getIndexCommitListener(),
                        config.isPromotableToPrimary(),
                        config.getMapperService(),
                        config.getEngineResetLock(),
                        config.getMergeMetrics(),
                        config.getIndexDeletionPolicyWrapper()
                    )
                )
            );
        }

        private MergePolicy wrapMergePolicy(MergePolicy policy) {
            if (blockMerges.get() == false) {
                return policy;
            }
            return new OneMergeWrappingMergePolicy(policy, toWrap -> new MergePolicy.OneMerge(toWrap) {

                void maybeBlockMerge() {
                    if (mergesStarted.compareAndSet(false, true)) {
                        mergesStartedLatch.countDown();
                    }
                    safeAwait(resumeMerges, TimeValue.ONE_HOUR);
                }

                @Override
                public CodecReader wrapForMerge(CodecReader reader) throws IOException {
                    return new FilterCodecReader(toWrap.wrapForMerge(reader)) {

                        @Override
                        public CacheHelper getReaderCacheHelper() {
                            return in.getReaderCacheHelper();
                        }

                        @Override
                        public CacheHelper getCoreCacheHelper() {
                            return in.getCoreCacheHelper();
                        }

                        @Override
                        public StoredFieldsReader getFieldsReader() {
                            return new WrappedStoredFieldsReader(super.getFieldsReader());
                        }

                        private class WrappedStoredFieldsReader extends StoredFieldsReader {

                            private final StoredFieldsReader delegate;

                            private WrappedStoredFieldsReader(StoredFieldsReader delegate) {
                                this.delegate = delegate;
                            }

                            @Override
                            public void checkIntegrity() throws IOException {
                                maybeBlockMerge();
                                delegate.checkIntegrity();
                                checkIntegrityCount.incrementAndGet();
                            }

                            @Override
                            public void close() throws IOException {
                                delegate.close();
                            }

                            @Override
                            public void document(int docID, StoredFieldVisitor visitor) throws IOException {
                                delegate.document(docID, visitor);
                                mergedDocsCount.incrementAndGet();
                            }

                            @Override
                            public StoredFieldsReader clone() {
                                return new WrappedStoredFieldsReader(delegate.clone());
                            }

                            @Override
                            public StoredFieldsReader getMergeInstance() {
                                return new WrappedStoredFieldsReader(delegate.getMergeInstance());
                            }
                        }

                        @Override
                        public DocValuesProducer getDocValuesReader() {
                            return new FilterDocValuesProducer(super.getDocValuesReader()) {
                                @Override
                                public NumericDocValues getNumeric(FieldInfo field) throws IOException {
                                    var result = super.getNumeric(field);
                                    mergedFieldsCount.incrementAndGet();
                                    return result;
                                }

                                @Override
                                public BinaryDocValues getBinary(FieldInfo field) throws IOException {
                                    var result = super.getBinary(field);
                                    mergedFieldsCount.incrementAndGet();
                                    return result;
                                }

                                @Override
                                public SortedDocValues getSorted(FieldInfo field) throws IOException {
                                    var result = super.getSorted(field);
                                    mergedFieldsCount.incrementAndGet();
                                    return result;
                                }

                                @Override
                                public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
                                    var result = super.getSortedNumeric(field);
                                    mergedFieldsCount.incrementAndGet();
                                    return result;
                                }

                                @Override
                                public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
                                    var result = super.getSortedSet(field);
                                    mergedFieldsCount.incrementAndGet();
                                    return result;
                                }

                                @Override
                                public void checkIntegrity() throws IOException {
                                    maybeBlockMerge();
                                    super.checkIntegrity();
                                    checkIntegrityCount.incrementAndGet();
                                }
                            };
                        }
                    };
                }
            });
        }
    }
}
