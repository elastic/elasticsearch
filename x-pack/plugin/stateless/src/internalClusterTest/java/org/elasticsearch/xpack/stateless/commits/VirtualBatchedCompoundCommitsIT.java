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
 */

package co.elastic.elasticsearch.stateless.commits;

import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;
import co.elastic.elasticsearch.stateless.IndexingDiskController;
import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.action.GetVirtualBatchedCompoundCommitChunkRequest;
import co.elastic.elasticsearch.stateless.action.TransportGetVirtualBatchedCompoundCommitChunkAction;
import co.elastic.elasticsearch.stateless.engine.IndexEngine;
import co.elastic.elasticsearch.stateless.engine.RefreshThrottler;
import co.elastic.elasticsearch.stateless.engine.SearchEngine;
import co.elastic.elasticsearch.stateless.engine.translog.TranslogReplicator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.ConnectTransportException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.LongConsumer;

import static co.elastic.elasticsearch.stateless.recovery.TransportStatelessPrimaryRelocationAction.START_RELOCATION_ACTION_NAME;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsStringIgnoringCase;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

public class VirtualBatchedCompoundCommitsIT extends AbstractStatelessIntegTestCase {

    /**
     * A plugin that produces file not found failures when the {@link co.elastic.elasticsearch.stateless.engine.IndexEngine} tries to read
     * a virtual batched compound commit chunk with a specific offset (Long.MAX_VALUE).
     */
    public static class TestStateless extends Stateless {
        public TestStateless(Settings settings) {
            super(settings);
        }

        @Override
        protected IndexEngine newIndexEngine(
            EngineConfig engineConfig,
            TranslogReplicator translogReplicator,
            Function<String, BlobContainer> translogBlobContainer,
            StatelessCommitService statelessCommitService,
            RefreshThrottler.Factory refreshThrottlerFactory,
            LongConsumer closedReadersForGenerationConsumer
        ) {
            return new IndexEngine(
                engineConfig,
                translogReplicator,
                translogBlobContainer,
                statelessCommitService,
                refreshThrottlerFactory,
                closedReadersForGenerationConsumer
            ) {
                @Override
                public void readVirtualBatchedCompoundCommitChunk(
                    final GetVirtualBatchedCompoundCommitChunkRequest request,
                    final BytesReference reference
                ) throws IOException {
                    if (request.getOffset() == Long.MAX_VALUE) {
                        throw randomFrom(new FileNotFoundException("simulated"), new NoSuchFileException("simulated"));
                    }
                    super.readVirtualBatchedCompoundCommitChunk(request, reference);
                }
            };
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

    public void testSearchGetVirtualBatchedCompoundCommitChunk() throws Exception {
        startMasterOnlyNode();
        startIndexNode();
        startSearchNode();
        final String indexName = randomIdentifier();
        createIndex(indexName, 1, 1);
        ensureGreen(indexName);

        final var index = resolveIndex(indexName);
        final var searchShard = findSearchShard(index, 0);
        final var searchEngine = getShardEngine(searchShard, SearchEngine.class);
        int length = randomIntBetween(5, 20);
        searchEngine.getVirtualBatchedCompoundCommitChunk(
            randomNonNegativeLong(),
            randomLongBetween(0, Long.MAX_VALUE - 1),
            length,
            ActionListener.wrap(response -> validateVirtualBatchedCompoundCommitResponse(response, length), e -> {
                assert false : e;
            })
        );
    }

    public void testSearchGetVirtualBatchedCompoundCommitChunkWaitsForPrimaryRelocation() throws Exception {
        startMasterOnlyNode();
        final var indexNodeA = startIndexNode();
        startSearchNode();
        var indexName = randomIdentifier();
        createIndex(indexName, 1, 1);
        ensureGreen(indexName);
        indexDocsAndRefresh(indexName, randomIntBetween(1, 5));

        startIndexNode();
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

        final var index = resolveIndex(indexName);
        final var searchShard = findSearchShard(index, 0);
        final var searchEngine = getShardEngine(searchShard, SearchEngine.class);
        int length = randomIntBetween(5, 20);
        CountDownLatch validated = new CountDownLatch(1);
        searchEngine.getVirtualBatchedCompoundCommitChunk(
            randomNonNegativeLong(),
            randomLongBetween(0, Long.MAX_VALUE - 1),
            length,
            ActionListener.wrap(response -> {
                validateVirtualBatchedCompoundCommitResponse(response, length);
                validated.countDown();
            }, e -> { assert false : e; })
        );
        actionSent.countDown();

        assertBusy(() -> assertThat(internalCluster().nodesInclude(indexName), not(hasItem(indexNodeA))));
        safeAwait(validated);
    }

    public void testSearchGetVirtualBatchedCompoundCommitChunkRetriesIfPrimaryRelocates() throws Exception {
        startMasterOnlyNode();
        final var indexNodeA = startIndexNode();
        final var searchNode = startSearchNode();
        var indexName = randomIdentifier();
        createIndex(indexName, 1, 1);
        ensureGreen(indexName);
        indexDocsAndRefresh(indexName, randomIntBetween(1, 5));

        startIndexNode();
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

        final var index = resolveIndex(indexName);
        final var searchShard = findSearchShard(index, 0);
        final var searchEngine = getShardEngine(searchShard, SearchEngine.class);
        int length = randomIntBetween(5, 20);
        CountDownLatch validated = new CountDownLatch(1);
        new Thread(() -> {
            searchEngine.getVirtualBatchedCompoundCommitChunk(
                randomNonNegativeLong(),
                randomLongBetween(0, Long.MAX_VALUE - 1),
                length,
                ActionListener.wrap(response -> {
                    validateVirtualBatchedCompoundCommitResponse(response, length);
                    validated.countDown();
                }, e -> { assert false : e; })
            );
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

    private void testSearchGetVirtualBatchedCompoundCommitChunkFailureType(FailureType failureType) throws Exception {
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

        final var index = resolveIndex(indexName);
        final var searchShard = findSearchShard(index, 0);
        final var searchEngine = getShardEngine(searchShard, SearchEngine.class);
        int length = randomIntBetween(5, 20);
        CountDownLatch exceptionThrown = new CountDownLatch(1);
        new Thread(() -> {
            searchEngine.getVirtualBatchedCompoundCommitChunk(
                randomNonNegativeLong(),
                randomLongBetween(0, Long.MAX_VALUE - 1),
                length,
                ActionListener.wrap(response -> {
                    assert false : "exception expected";
                }, e -> {
                    assertThat(
                        e.getCause(),
                        instanceOf(failureType == FailureType.INDEX_CLOSED ? IndexClosedException.class : IndexNotFoundException.class)
                    );
                    exceptionThrown.countDown();
                })
            );
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

    public void testSearchGetVirtualBatchedCompoundCommitChunkFailureWhenIndexIsDeleted() throws Exception {
        testSearchGetVirtualBatchedCompoundCommitChunkFailureType(FailureType.INDEX_NOT_FOUND_WHEN_INDEX_DELETED);
    }

    public void testSearchGetVirtualBatchedCompoundCommitChunkFailureWhenIndexCloses() throws Exception {
        testSearchGetVirtualBatchedCompoundCommitChunkFailureType(FailureType.INDEX_CLOSED);
    }

    private void testSearchGetVirtualBatchedCompoundCommitChunkFailureDuringPrimaryRelocation(FailureType failureType) throws Exception {
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

        final var index = resolveIndex(indexName);
        final var searchShard = findSearchShard(index, 0);
        final var searchEngine = getShardEngine(searchShard, SearchEngine.class);
        int length = randomIntBetween(5, 20);
        CountDownLatch exceptionThrown = new CountDownLatch(1);
        searchEngine.getVirtualBatchedCompoundCommitChunk(
            randomNonNegativeLong(),
            randomLongBetween(0, Long.MAX_VALUE - 1),
            length,
            ActionListener.wrap(response -> {
                assert false : "exception expected";
            }, e -> {
                if (failureType == FailureType.INDEX_CLOSED) {
                    assertThat(e.getCause(), instanceOf(IndexClosedException.class));
                } else {
                    assertThat(e, instanceOf(IndexNotFoundException.class));
                }
                exceptionThrown.countDown();
            })
        );

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

    public void testSearchGetVirtualBatchedCompoundCommitChunkFailureWhenIndexIsDeletedDuringPrimaryRelocation() throws Exception {
        testSearchGetVirtualBatchedCompoundCommitChunkFailureDuringPrimaryRelocation(FailureType.INDEX_NOT_FOUND_WHEN_INDEX_DELETED);
    }

    public void testSearchGetVirtualBatchedCompoundCommitChunkFailureWhenIndexClosesDuringPrimaryRelocation() throws Exception {
        testSearchGetVirtualBatchedCompoundCommitChunkFailureDuringPrimaryRelocation(FailureType.INDEX_CLOSED);
    }

    public void testSearchGetVirtualBatchedCompoundCommitChunkRetryConnectivity() throws Exception {
        startMasterOnlyNode();
        final var indexNode = startIndexNode();
        startSearchNode();
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

        final var searchShard = findSearchShard(index, 0);
        final var searchEngine = getShardEngine(searchShard, SearchEngine.class);
        int length = randomIntBetween(5, 20);
        CountDownLatch validated = new CountDownLatch(1);
        searchEngine.getVirtualBatchedCompoundCommitChunk(
            randomNonNegativeLong(),
            randomLongBetween(0, Long.MAX_VALUE - 1),
            length,
            ActionListener.wrap(response -> {
                validateVirtualBatchedCompoundCommitResponse(response, length);
                validated.countDown();
            }, e -> { assert false : e; })
        );
        safeAwait(validated);
    }

    public void testSearchGetVirtualBatchedCompoundCommitChunkFileError() throws Exception {
        startMasterOnlyNode();
        final var indexNode = startIndexNode();
        startSearchNode();
        var indexName = randomIdentifier();
        createIndex(indexName, 1, 1);
        ensureGreen(indexName);
        indexDocsAndRefresh(indexName, randomIntBetween(1, 5));

        final var index = resolveIndex(indexName);
        long primaryTermStart = findIndexShard(index, 0).getOperationPrimaryTerm();
        final var searchShard = findSearchShard(index, 0);
        final var searchEngine = getShardEngine(searchShard, SearchEngine.class);
        int length = randomIntBetween(5, 20);
        CountDownLatch exceptionThrown = new CountDownLatch(1);
        searchEngine.getVirtualBatchedCompoundCommitChunk(randomNonNegativeLong(), Long.MAX_VALUE, length, ActionListener.wrap(response -> {
            assert false : "exception expected";
        }, e -> {
            assertThat(e.getCause(), either(instanceOf(FileNotFoundException.class)).or(instanceOf(NoSuchFileException.class)));
            exceptionThrown.countDown();
        }));
        safeAwait(exceptionThrown);
        assertBusy(() -> {
            long primaryTermEnd = findIndexShard(index, 0).getOperationPrimaryTerm();
            assertThat("failed shard should be re-allocated with new primary term", primaryTermEnd, greaterThan(primaryTermStart));
        });
    }

    public void testSearchGetVirtualBatchedCompoundCommitChunkFailWithWrongPrimaryTerm() throws Exception {
        startMasterOnlyNode();
        startIndexNode();
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
        final var searchShard = findSearchShard(index, 0);
        final var searchEngine = getShardEngine(searchShard, SearchEngine.class);
        int length = randomIntBetween(5, 20);
        CountDownLatch exceptionThrown = new CountDownLatch(1);
        new Thread(() -> {
            searchEngine.getVirtualBatchedCompoundCommitChunk(
                randomNonNegativeLong(),
                randomLongBetween(0, Long.MAX_VALUE - 1),
                length,
                ActionListener.wrap(response -> {
                    assert false : "exception expected";
                }, e -> {
                    assertThat(e.getCause(), instanceOf(ElasticsearchException.class));
                    assertThat(e.getCause().getMessage(), containsStringIgnoringCase("primary term mismatch"));
                    exceptionThrown.countDown();
                })
            );
        }).start();

        safeAwait(actionSent);
        final var indexShard = findIndexShard(index, 0);
        long primaryTerm = indexShard.getOperationPrimaryTerm();
        indexShard.failShard("test simulated", new Exception("test simulated"));
        assertBusy(() -> assertThat(findIndexShard(index, 0).getOperationPrimaryTerm(), greaterThan(primaryTerm)));
        relocationCompleted.countDown();
        safeAwait(exceptionThrown);
    }

    private void validateVirtualBatchedCompoundCommitResponse(ReleasableBytesReference response, int length) {
        // TODO validate the real bytes from the indirection layer of the primary shard once we have the way to do that
        try (ReleasableBytesReference reference = response.retain()) {
            BytesRef referenceBytes = reference.toBytesRef();
            byte[] referenceArray = referenceBytes.bytes;
            int referenceOffset = referenceBytes.offset;
            assert reference.length() == length : "length " + reference.length();
            byte b = 1;
            for (int i = 0; i < reference.length(); i++) {
                byte value = referenceArray[i + referenceOffset];
                assert value == b : "value " + value + " at " + i + " with offset " + referenceOffset + " vs " + b;
                b++;
            }
        }
    }
}
