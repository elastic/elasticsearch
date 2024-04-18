/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 *
 * This file was contributed to by generative AI
 */

package co.elastic.elasticsearch.stateless.commits;

import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;
import co.elastic.elasticsearch.stateless.IndexingDiskController;
import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.action.GetVirtualBatchedCompoundCommitChunkRequest;
import co.elastic.elasticsearch.stateless.action.NewCommitNotificationRequest;
import co.elastic.elasticsearch.stateless.action.TransportGetVirtualBatchedCompoundCommitChunkAction;
import co.elastic.elasticsearch.stateless.action.TransportNewCommitNotificationAction;
import co.elastic.elasticsearch.stateless.cache.reader.IndexingShardCacheBlobReader;
import co.elastic.elasticsearch.stateless.engine.IndexEngine;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;
import co.elastic.elasticsearch.stateless.engine.RefreshThrottler;
import co.elastic.elasticsearch.stateless.engine.translog.TranslogReplicator;
import co.elastic.elasticsearch.stateless.lucene.SearchDirectory;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.node.PluginComponentBinding;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.ConnectTransportException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static co.elastic.elasticsearch.stateless.cache.reader.CacheBlobReaderService.TRANSPORT_BLOB_READER_CHUNK_SIZE_SETTING;
import static co.elastic.elasticsearch.stateless.recovery.TransportStatelessPrimaryRelocationAction.START_RELOCATION_ACTION_NAME;
import static org.elasticsearch.blobcache.shared.SharedBytes.PAGE_SIZE;
import static org.elasticsearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.hamcrest.Matchers.containsStringIgnoringCase;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

public class VirtualBatchedCompoundCommitsIT extends AbstractStatelessIntegTestCase {

    /**
     * A plugin that gives the ability to override the
     * {@link IndexEngine#readVirtualBatchedCompoundCommitChunk(GetVirtualBatchedCompoundCommitChunkRequest, StreamOutput)} function on an
     * indexing node to either:
     * - produce file not found failures when given offset is of a specific value (Long.MAX_VALUE)
     * - otherwise, simulate the returned bytes with a certain pattern that can be validated with the
     *   validateSimulatedVirtualBatchedCompoundCommitChunkResponse function
     *
     * Also, the plugin customizes {@link StatelessCommitService} to pinpoint the last virtual batched compound commit to be uploaded.
     */
    public static class TestStateless extends Stateless {
        public volatile TestStatelessCommitService statelessCommitService;
        private volatile boolean simulateReadVirtualBatchedCompoundCommitChunk = false;

        public TestStateless(Settings settings) {
            super(settings);
        }

        @Override
        protected IndexEngine newIndexEngine(
            EngineConfig engineConfig,
            TranslogReplicator translogReplicator,
            Function<String, BlobContainer> translogBlobContainer,
            StatelessCommitService statelessCommitService,
            RefreshThrottler.Factory refreshThrottlerFactory
        ) {
            return new IndexEngine(
                engineConfig,
                translogReplicator,
                translogBlobContainer,
                statelessCommitService,
                refreshThrottlerFactory,
                statelessCommitService.getIndexEngineLocalReaderListenerForShard(engineConfig.getShardId()),
                statelessCommitService.getCommitBCCResolverForShard(engineConfig.getShardId())
            ) {
                @Override
                public void readVirtualBatchedCompoundCommitChunk(
                    final GetVirtualBatchedCompoundCommitChunkRequest request,
                    final StreamOutput output
                ) throws IOException {
                    if (simulateReadVirtualBatchedCompoundCommitChunk) {
                        if (request.getOffset() == Long.MAX_VALUE) {
                            throw randomFrom(new FileNotFoundException("simulated"), new NoSuchFileException("simulated"));
                        }
                        byte b = 1;
                        for (int i = 0; i < request.getLength(); i++) {
                            output.write(b++);
                        }
                    } else {
                        super.readVirtualBatchedCompoundCommitChunk(request, output);
                    }
                }
            };
        }

        public static TestStateless getPlugin(String node) {
            return internalCluster().getInstance(PluginsService.class, node).filterPlugins(TestStateless.class).findFirst().get();
        }

        public static void enableSimulationOfReadVirtualBatchedCompoundCommitChunk(String node) {
            getPlugin(node).simulateReadVirtualBatchedCompoundCommitChunk = true;
        }

        @Override
        public Collection<Object> createComponents(PluginServices services) {
            final Collection<Object> components = super.createComponents(services);
            components.add(
                new PluginComponentBinding<>(
                    StatelessCommitService.class,
                    components.stream().filter(c -> c instanceof TestStatelessCommitService).findFirst().orElseThrow()
                )
            );
            return components;
        }

        @Override
        protected StatelessCommitService createStatelessCommitService(
            Settings settings,
            ObjectStoreService objectStoreService,
            ClusterService clusterService,
            Client client,
            StatelessCommitCleaner commitCleaner
        ) {
            statelessCommitService = new TestStatelessCommitService(settings, objectStoreService, clusterService, client, commitCleaner);
            return statelessCommitService;
        }
    }

    public static class TestStatelessCommitService extends StatelessCommitService {
        public volatile VirtualBatchedCompoundCommit lastVirtualBccToBeUploaded = null;

        public TestStatelessCommitService(
            Settings settings,
            ObjectStoreService objectStoreService,
            ClusterService clusterService,
            Client client,
            StatelessCommitCleaner commitCleaner
        ) {
            super(settings, objectStoreService, clusterService, client, commitCleaner);
        }

        @Override
        protected ShardCommitState createShardCommitState(ShardId shardId, long primaryTerm) {
            return new ShardCommitState(shardId, primaryTerm) {
                @Override
                protected boolean shouldUploadVirtualBcc(VirtualBatchedCompoundCommit virtualBcc) {
                    if (super.shouldUploadVirtualBcc(virtualBcc)) {
                        lastVirtualBccToBeUploaded = virtualBcc;
                        return true;
                    }
                    return false;
                }
            };
        }

        @Override
        public VirtualBatchedCompoundCommit getVirtualBatchedCompoundCommit(
            ShardId shardId,
            PrimaryTermAndGeneration primaryTermAndGeneration
        ) {
            if (lastVirtualBccToBeUploaded != null) {
                return lastVirtualBccToBeUploaded;
            }
            return super.getVirtualBatchedCompoundCommit(shardId, primaryTermAndGeneration);
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(Stateless.class);
        plugins.add(TestStateless.class);
        return plugins;
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(IndexingDiskController.INDEXING_DISK_INTERVAL_TIME_SETTING.getKey(), TimeValue.timeValueHours(1L));
    }

    public void testSimulatedGetVirtualBatchedCompoundCommitChunk() throws Exception {
        startMasterOnlyNode();
        final var indexNode = startIndexNode();
        TestStateless.enableSimulationOfReadVirtualBatchedCompoundCommitChunk(indexNode);
        var searchNode = startSearchNode();
        final String indexName = randomIdentifier();
        createIndex(indexName, 1, 1);
        ensureGreen(indexName);

        int length = randomIntBetween(5, 20);
        var indexingShardCacheBlobReader = new IndexingShardCacheBlobReader(
            findSearchShard(indexName).shardId(),
            new PrimaryTermAndGeneration(1, randomNonNegativeLong()),
            client(searchNode),
            ByteSizeValue.ofBytes(PAGE_SIZE * randomIntBetween(1, 256)) // 4KiB to 1MiB
        );
        try (var inputStream = indexingShardCacheBlobReader.getRangeInputStream(randomLongBetween(0, Long.MAX_VALUE - 1), length)) {
            validateSimulatedVirtualBatchedCompoundCommitChunkResponse(inputStream, length);
        }
    }

    public void testSimulatedGetVirtualBatchedCompoundCommitChunkWaitsForPrimaryRelocation() throws Exception {
        startMasterOnlyNode();
        final var indexNodeA = startIndexNode();
        TestStateless.enableSimulationOfReadVirtualBatchedCompoundCommitChunk(indexNodeA);
        var searchNode = startSearchNode();
        var indexName = randomIdentifier();
        createIndex(indexName, 1, 1);
        ensureGreen(indexName);
        indexDocsAndRefresh(indexName, randomIntBetween(1, 5));

        final var indexNodeB = startIndexNode();
        TestStateless.enableSimulationOfReadVirtualBatchedCompoundCommitChunk(indexNodeB);
        ensureStableCluster(4);

        final var transportService = MockTransportService.getInstance(indexNodeA);
        CountDownLatch relocationStarted = new CountDownLatch(1);
        CountDownLatch actionSent = new CountDownLatch(1);
        transportService.addRequestHandlingBehavior(START_RELOCATION_ACTION_NAME, (handler, request, channel, task) -> {
            relocationStarted.countDown();
            safeAwait(actionSent);
            handler.messageReceived(request, channel, task);
        });

        logger.info("--> excluding {}", indexNodeA);
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", indexNodeA), indexName);
        safeAwait(relocationStarted);

        int length = randomIntBetween(5, 20);
        var indexingShardCacheBlobReader = new IndexingShardCacheBlobReader(
            findSearchShard(indexName).shardId(),
            new PrimaryTermAndGeneration(1, randomNonNegativeLong()),
            client(searchNode),
            TRANSPORT_BLOB_READER_CHUNK_SIZE_SETTING.get(nodeSettings().build())
        );
        CountDownLatch validated = new CountDownLatch(1);

        new Thread(() -> {
            try (var inputStream = indexingShardCacheBlobReader.getRangeInputStream(randomLongBetween(0, Long.MAX_VALUE - 1), length)) {
                validateSimulatedVirtualBatchedCompoundCommitChunkResponse(inputStream, length);
                validated.countDown();
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }).start();
        actionSent.countDown();

        assertBusy(() -> assertThat(internalCluster().nodesInclude(indexName), not(hasItem(indexNodeA))));
        safeAwait(validated);
    }

    public void testSimulatedGetVirtualBatchedCompoundCommitChunkRetriesIfPrimaryRelocates() throws Exception {
        startMasterOnlyNode();
        final var indexNodeA = startIndexNode();
        TestStateless.enableSimulationOfReadVirtualBatchedCompoundCommitChunk(indexNodeA);
        final var searchNode = startSearchNode();
        var indexName = randomIdentifier();
        createIndex(indexName, 1, 1);
        ensureGreen(indexName);
        indexDocsAndRefresh(indexName, randomIntBetween(1, 5));

        final var indexNodeB = startIndexNode();
        TestStateless.enableSimulationOfReadVirtualBatchedCompoundCommitChunk(indexNodeB);
        ensureStableCluster(4);

        CountDownLatch actionAppeared = new CountDownLatch(1);
        CountDownLatch actionSent = new CountDownLatch(1);
        final var transportService = MockTransportService.getInstance(searchNode);
        transportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (connection.getNode().getName().equals(indexNodeA)
                && action.equals(TransportGetVirtualBatchedCompoundCommitChunkAction.NAME + "[p]")) {
                actionAppeared.countDown();
                safeAwait(actionSent);
            }
            connection.sendRequest(requestId, action, request, options);
        });

        var indexingShardCacheBlobReader = new IndexingShardCacheBlobReader(
            findSearchShard(indexName).shardId(),
            new PrimaryTermAndGeneration(1, randomNonNegativeLong()),
            client(searchNode),
            TRANSPORT_BLOB_READER_CHUNK_SIZE_SETTING.get(nodeSettings().build())
        );
        int length = randomIntBetween(5, 20);
        CountDownLatch validated = new CountDownLatch(1);
        new Thread(() -> {
            try (var inputStream = indexingShardCacheBlobReader.getRangeInputStream(randomLongBetween(0, Long.MAX_VALUE - 1), length)) {
                validateSimulatedVirtualBatchedCompoundCommitChunkResponse(inputStream, length);
                validated.countDown();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).start();

        safeAwait(actionAppeared);
        // Relocate primary, which will make the action retry due to the IndexNotFoundException on the original indexing node
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", indexNodeA), indexName);
        assertBusy(() -> assertThat(internalCluster().nodesInclude(indexName), not(hasItem(indexNodeA))));
        actionSent.countDown();
        safeAwait(validated);
    }

    private enum FailureType {
        INDEX_NOT_FOUND_WHEN_INDEX_DELETED,
        INDEX_CLOSED
    }

    private void testGetVirtualBatchedCompoundCommitChunkFailureType(FailureType failureType) throws Exception {
        startMasterOnlyNode();
        startIndexNode();
        final var searchNode = startSearchNode();
        var indexName = randomIdentifier();
        createIndex(indexName, 1, 1);
        ensureGreen(indexName);
        indexDocsAndRefresh(indexName, randomIntBetween(1, 5));

        CountDownLatch actionAppeared = new CountDownLatch(1);
        CountDownLatch actionSent = new CountDownLatch(1);
        final var transportService = MockTransportService.getInstance(searchNode);
        transportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(TransportGetVirtualBatchedCompoundCommitChunkAction.NAME + "[p]")) {
                actionAppeared.countDown();
                safeAwait(actionSent);
            }
            connection.sendRequest(requestId, action, request, options);
        });

        var indexingShardCacheBlobReader = new IndexingShardCacheBlobReader(
            findSearchShard(indexName).shardId(),
            new PrimaryTermAndGeneration(1, randomNonNegativeLong()),
            client(searchNode),
            TRANSPORT_BLOB_READER_CHUNK_SIZE_SETTING.get(nodeSettings().build())
        );
        int length = randomIntBetween(5, 20);
        CountDownLatch exceptionThrown = new CountDownLatch(1);
        new Thread(() -> {
            try (var inputStream = indexingShardCacheBlobReader.getRangeInputStream(randomLongBetween(0, Long.MAX_VALUE - 1), length)) {
                assert false : "exception expected";
            } catch (Exception e) {
                assertThat(
                    e.getCause(),
                    instanceOf(failureType == FailureType.INDEX_CLOSED ? IndexClosedException.class : IndexNotFoundException.class)
                );
                exceptionThrown.countDown();
            }
        }).start();

        safeAwait(actionAppeared);
        switch (failureType) {
            case INDEX_NOT_FOUND_WHEN_INDEX_DELETED:
                // Delete the index, which will make the action fail with IndexNotFoundException on the indexing node
                assertAcked(indicesAdmin().delete(new DeleteIndexRequest(indexName)).actionGet());
                break;
            case INDEX_CLOSED:
                // Close the index, which will make the action fail with IndexClosedException on the indexing node
                assertAcked(indicesAdmin().close(new CloseIndexRequest(indexName)).actionGet());
                break;
            default:
                assert false : "unexpected failure type: " + failureType;
        }
        actionSent.countDown();
        safeAwait(exceptionThrown);
    }

    public void testGetVirtualBatchedCompoundCommitChunkFailureWhenIndexIsDeleted() throws Exception {
        testGetVirtualBatchedCompoundCommitChunkFailureType(FailureType.INDEX_NOT_FOUND_WHEN_INDEX_DELETED);
    }

    public void testGetVirtualBatchedCompoundCommitChunkFailureWhenIndexCloses() throws Exception {
        testGetVirtualBatchedCompoundCommitChunkFailureType(FailureType.INDEX_CLOSED);
    }

    private void testGetVirtualBatchedCompoundCommitChunkFailureDuringPrimaryRelocation(FailureType failureType) throws Exception {
        startMasterOnlyNode();
        final var indexNodeA = startIndexNode();
        final var searchNode = startSearchNode();
        var indexName = randomIdentifier();
        createIndex(indexName, 1, 1);
        ensureGreen(indexName);
        indexDocsAndRefresh(indexName, randomIntBetween(1, 5));

        startIndexNode();
        ensureStableCluster(4);

        final var IndexNodeATransportService = MockTransportService.getInstance(indexNodeA);
        CountDownLatch relocationStarted = new CountDownLatch(1);
        CountDownLatch continueRelocation = new CountDownLatch(1);
        IndexNodeATransportService.addRequestHandlingBehavior(START_RELOCATION_ACTION_NAME, (handler, request, channel, task) -> {
            relocationStarted.countDown();
            safeAwait(continueRelocation);
            handler.messageReceived(request, channel, task);
        });

        logger.info("--> excluding {}", indexNodeA);
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", indexNodeA), indexName);
        safeAwait(relocationStarted);

        var indexingShardCacheBlobReader = new IndexingShardCacheBlobReader(
            findSearchShard(indexName).shardId(),
            new PrimaryTermAndGeneration(1, randomNonNegativeLong()),
            client(searchNode),
            TRANSPORT_BLOB_READER_CHUNK_SIZE_SETTING.get(nodeSettings().build())
        );
        int length = randomIntBetween(5, 20);
        CountDownLatch exceptionThrown = new CountDownLatch(1);
        new Thread(() -> {
            try (var inputStream = indexingShardCacheBlobReader.getRangeInputStream(randomLongBetween(0, Long.MAX_VALUE - 1), length)) {
                assert false : "exception expected";
            } catch (Exception e) {
                if (failureType == FailureType.INDEX_CLOSED) {
                    assertThat(e.getCause(), instanceOf(IndexClosedException.class));
                } else {
                    assertThat(e, instanceOf(IndexNotFoundException.class));
                }
                exceptionThrown.countDown();
            }
        }).start();

        switch (failureType) {
            case INDEX_NOT_FOUND_WHEN_INDEX_DELETED:
                // Delete the index, which will make the action's ClusterStateObserver fail with IndexNotFoundException
                assertAcked(indicesAdmin().delete(new DeleteIndexRequest(indexName)).actionGet());
                break;
            case INDEX_CLOSED:
                // Close the index, which will make the action fail with IndexClosedException
                assertAcked(indicesAdmin().close(new CloseIndexRequest(indexName)).actionGet());
                break;
            default:
                assert false : "unexpected failure type: " + failureType;
        }
        continueRelocation.countDown();
        safeAwait(exceptionThrown); // does not depend on the `continueRelocation.countDown()` command necessarily
    }

    public void testGetVirtualBatchedCompoundCommitChunkFailureWhenIndexIsDeletedDuringPrimaryRelocation() throws Exception {
        testGetVirtualBatchedCompoundCommitChunkFailureDuringPrimaryRelocation(FailureType.INDEX_NOT_FOUND_WHEN_INDEX_DELETED);
    }

    public void testGetVirtualBatchedCompoundCommitChunkFailureWhenIndexClosesDuringPrimaryRelocation() throws Exception {
        testGetVirtualBatchedCompoundCommitChunkFailureDuringPrimaryRelocation(FailureType.INDEX_CLOSED);
    }

    public void testSimulatedGetVirtualBatchedCompoundCommitChunkRetryConnectivity() throws Exception {
        startMasterOnlyNode();
        final var indexNode = startIndexNode();
        TestStateless.enableSimulationOfReadVirtualBatchedCompoundCommitChunk(indexNode);
        var searchNode = startSearchNode();
        var indexName = randomIdentifier();
        createIndex(indexName, 1, 1);
        ensureGreen(indexName);
        indexDocsAndRefresh(indexName, randomIntBetween(1, 5));
        final var index = resolveIndex(indexName);
        final var indexShard = findIndexShard(index, 0);

        AtomicInteger counter = new AtomicInteger(0);
        final var transportService = MockTransportService.getInstance(indexNode);
        transportService.addRequestHandlingBehavior(
            TransportGetVirtualBatchedCompoundCommitChunkAction.NAME + "[p]",
            (handler, request, channel, task) -> {
                if (counter.getAndIncrement() == 0) {
                    channel.sendResponse(
                        randomFrom(
                            new ConnectTransportException(transportService.getLocalNode(), "simulated"),
                            new CircuitBreakingException("Simulated", CircuitBreaker.Durability.TRANSIENT),
                            new NodeClosedException(transportService.getLocalNode()),
                            new ShardNotFoundException(indexShard.shardId())
                        )
                    );
                } else {
                    handler.messageReceived(request, channel, task);
                }
            }
        );

        var indexingShardCacheBlobReader = new IndexingShardCacheBlobReader(
            findSearchShard(indexName).shardId(),
            new PrimaryTermAndGeneration(1, randomNonNegativeLong()),
            client(searchNode),
            TRANSPORT_BLOB_READER_CHUNK_SIZE_SETTING.get(nodeSettings().build())
        );
        int length = randomIntBetween(5, 20);
        try (var inputStream = indexingShardCacheBlobReader.getRangeInputStream(randomLongBetween(0, Long.MAX_VALUE - 1), length)) {
            validateSimulatedVirtualBatchedCompoundCommitChunkResponse(inputStream, length);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void testSimulatedGetVirtualBatchedCompoundCommitChunkFileError() throws Exception {
        startMasterOnlyNode();
        final var indexNodeA = startIndexNode();
        TestStateless.enableSimulationOfReadVirtualBatchedCompoundCommitChunk(indexNodeA);
        var searchNode = startSearchNode();
        var indexName = randomIdentifier();
        createIndex(indexName, 1, 1);
        ensureGreen(indexName);
        indexDocsAndRefresh(indexName, randomIntBetween(1, 5));

        final var index = resolveIndex(indexName);
        long primaryTermStart = findIndexShard(index, 0).getOperationPrimaryTerm();
        var indexingShardCacheBlobReader = new IndexingShardCacheBlobReader(
            findSearchShard(indexName).shardId(),
            new PrimaryTermAndGeneration(1, randomNonNegativeLong()),
            client(searchNode),
            TRANSPORT_BLOB_READER_CHUNK_SIZE_SETTING.get(nodeSettings().build())
        );
        int length = randomIntBetween(5, 20);
        try (var inputStream = indexingShardCacheBlobReader.getRangeInputStream(Long.MAX_VALUE, length)) {
            assert false : "exception expected";
        } catch (Exception e) {
            assertThat(e.getCause(), either(instanceOf(FileNotFoundException.class)).or(instanceOf(NoSuchFileException.class)));
        }
        assertBusy(() -> {
            long primaryTermEnd = findIndexShard(index, 0).getOperationPrimaryTerm();
            assertThat("failed shard should be re-allocated with new primary term", primaryTermEnd, greaterThan(primaryTermStart));
        });
    }

    public void testGetVirtualBatchedCompoundCommitChunkFailWithWrongPrimaryTerm() throws Exception {
        startMasterOnlyNode();
        final var indexNodeA = startIndexNode();
        TestStateless.enableSimulationOfReadVirtualBatchedCompoundCommitChunk(indexNodeA);
        final var searchNode = startSearchNode();
        var indexName = randomIdentifier();
        createIndex(indexName, 1, 1);
        ensureGreen(indexName);
        indexDocsAndRefresh(indexName, randomIntBetween(1, 5));

        CountDownLatch actionSent = new CountDownLatch(1);
        CountDownLatch relocationCompleted = new CountDownLatch(1);
        final var transportService = MockTransportService.getInstance(searchNode);
        transportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(TransportGetVirtualBatchedCompoundCommitChunkAction.NAME + "[p]")) {
                actionSent.countDown();
                safeAwait(relocationCompleted);
            }
            connection.sendRequest(requestId, action, request, options);
        });

        final var index = resolveIndex(indexName);
        var indexingShardCacheBlobReader = new IndexingShardCacheBlobReader(
            findSearchShard(indexName).shardId(),
            new PrimaryTermAndGeneration(1, randomNonNegativeLong()),
            client(searchNode),
            TRANSPORT_BLOB_READER_CHUNK_SIZE_SETTING.get(nodeSettings().build())
        );
        int length = randomIntBetween(5, 20);
        CountDownLatch exceptionThrown = new CountDownLatch(1);
        new Thread(() -> {
            try (var inputStream = indexingShardCacheBlobReader.getRangeInputStream(randomLongBetween(0, Long.MAX_VALUE - 1), length)) {
                assert false : "exception expected";
            } catch (Exception e) {
                assertThat(e.getCause(), instanceOf(ElasticsearchException.class));
                assertThat(e.getCause().getMessage(), containsStringIgnoringCase("primary term mismatch"));
                exceptionThrown.countDown();
            }
        }).start();

        safeAwait(actionSent);
        final var indexShard = findIndexShard(index, 0);
        long primaryTerm = indexShard.getOperationPrimaryTerm();
        indexShard.failShard("test simulated", new Exception("test simulated"));
        assertBusy(() -> assertThat(findIndexShard(index, 0).getOperationPrimaryTerm(), greaterThan(primaryTerm)));
        relocationCompleted.countDown();
        safeAwait(exceptionThrown);
    }

    // TODO - Once multi-CC VBCCs work completely, simplify testing: remove forcing the search node to consider a last uploaded.
    // Simply ensure the search works by counting the correct number of docs, which means it calls correctly
    // the get chunk action from the indexing node. Also remove the
    // `VirtualBatchedCompoundCommitsIT.TestStatelessCommitService.getVirtualBatchedCompoundCommit` function.
    public void testGetVirtualBatchedCompoundCommitChunkOnLastVbcc() throws Exception {
        startMasterOnlyNode();
        var indexNode = startIndexNode();
        var searchNode = startSearchNode();
        final String indexName = randomIdentifier();
        createIndex(indexName, 1, 1);
        ensureGreen(indexName);

        assertNoFailuresAndResponse(
            prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()),
            searchResponse -> assertEquals(0L, searchResponse.getHits().getTotalHits().value)
        );

        var plugin = TestStateless.getPlugin(indexNode);

        var indexShard = findIndexShard(indexName);
        var indexEngine = ((IndexEngine) indexShard.getEngineOrNull());
        var lastUploadedTermAndGen = new PrimaryTermAndGeneration(indexShard.getOperationPrimaryTerm(), indexEngine.getCurrentGeneration());
        final var searchShard = findSearchShard(indexName);
        final var searchDirectory = SearchDirectory.unwrapDirectory(searchShard.store().directory());

        int totalDocs = randomIntBetween(1, 10);
        indexDocs(indexName, totalDocs);

        AtomicInteger getVbccChunkActions = new AtomicInteger(0);
        MockTransportService.getInstance(searchNode).addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(TransportGetVirtualBatchedCompoundCommitChunkAction.NAME + "[p]")) {
                getVbccChunkActions.incrementAndGet();
            }
            connection.sendRequest(requestId, action, request, options);
        });

        MockTransportService.getInstance(indexNode).addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.startsWith(TransportNewCommitNotificationAction.NAME)) {
                assert plugin.statelessCommitService.lastVirtualBccToBeUploaded != null
                    : "the rolling capability has been enabled so this test should be redone, see TODO at the top of the integration test";
                plugin.statelessCommitService.lastVirtualBccToBeUploaded.incRef();
            }
            connection.sendRequest(requestId, action, request, options);
        });

        MockTransportService.getInstance(searchNode)
            .addRequestHandlingBehavior(TransportNewCommitNotificationAction.NAME + "[u]", (handler, request, channel, task) -> {
                NewCommitNotificationRequest oldRequest = (NewCommitNotificationRequest) request;
                // Force the search node to consider that the last uploaded BCC is the one from before
                var newRequest = new NewCommitNotificationRequest(
                    IndexShardRoutingTable.builder(oldRequest.shardId())
                        .addShard(newShardRouting(oldRequest.shardId(), "_node", true, ShardRoutingState.STARTED))
                        .build(),
                    oldRequest.getCompoundCommit(),
                    oldRequest.getBatchedCompoundCommitGeneration(),
                    lastUploadedTermAndGen
                ) {
                    @Override
                    public boolean isUploaded() {
                        return true;
                    }
                };
                handler.messageReceived(newRequest, channel, task);
            });

        refresh(indexName);

        assertNoFailuresAndResponse(
            prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()),
            searchResponse -> assertEquals(totalDocs, searchResponse.getHits().getTotalHits().value)
        );

        plugin.statelessCommitService.lastVirtualBccToBeUploaded.decRef();
        assertThat(getVbccChunkActions.get(), greaterThan(0));
    }

    private void validateSimulatedVirtualBatchedCompoundCommitChunkResponse(InputStream inputStream, int length) throws IOException {
        var bytes = inputStream.readAllBytes();
        assert bytes.length == length : "length " + bytes.length;
        byte b = 1;
        for (int i = 0; i < bytes.length; i++) {
            byte value = bytes[i];
            assert value == b : "value " + value + " at " + i + " vs " + b;
            b++;
        }
    }
}
