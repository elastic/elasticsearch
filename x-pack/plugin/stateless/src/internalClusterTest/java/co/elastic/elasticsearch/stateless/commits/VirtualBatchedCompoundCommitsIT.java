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
import co.elastic.elasticsearch.stateless.action.TransportGetVirtualBatchedCompoundCommitChunkAction;
import co.elastic.elasticsearch.stateless.engine.SearchEngine;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.ConnectTransportException;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static co.elastic.elasticsearch.stateless.recovery.TransportStatelessPrimaryRelocationAction.START_RELOCATION_ACTION_NAME;
import static org.hamcrest.Matchers.containsStringIgnoringCase;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

public class VirtualBatchedCompoundCommitsIT extends AbstractStatelessIntegTestCase {

    public void testSearchGetVirtualBatchedCompoundCommitChunk() throws Exception {
        startMasterOnlyNode();
        startIndexNode(
            Settings.builder()
                .put(IndexingDiskController.INDEXING_DISK_INTERVAL_TIME_SETTING.getKey(), TimeValue.timeValueHours(1L))
                .build()
        );
        startSearchNode();
        final String indexName = randomIdentifier();
        createIndex(indexName, 1, 1);
        ensureGreen(indexName);

        final var index = resolveIndex(indexName);
        final var searchShard = findSearchShard(index, 0);
        final var searchEngine = getShardEngine(searchShard, SearchEngine.class);
        int length = randomIntBetween(5, 20);
        searchEngine.getVirtualBatchedCompoundCommitChunk(
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            length,
            ActionListener.wrap(response -> validateVirtualBatchedCompoundCommitResponse(response, length), e -> {
                assert false : e;
            })
        );
    }

    public void testSearchGetVirtualBatchedCompoundCommitChunkDuringPrimaryRelocation() throws Exception {
        startMasterOnlyNode();
        final var indexNodeA = startIndexNode(
            Settings.builder()
                .put(IndexingDiskController.INDEXING_DISK_INTERVAL_TIME_SETTING.getKey(), TimeValue.timeValueHours(1L))
                .build()
        );
        startSearchNode();
        var indexName = randomIdentifier();
        createIndex(indexName, 1, 1);
        ensureGreen(indexName);
        indexDocsAndRefresh(indexName, randomIntBetween(1, 5));

        startIndexNode(
            Settings.builder()
                .put(IndexingDiskController.INDEXING_DISK_INTERVAL_TIME_SETTING.getKey(), TimeValue.timeValueHours(1L))
                .build()
        );
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
            randomNonNegativeInt(),
            randomNonNegativeInt(),
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

    public void testSearchGetVirtualBatchedCompoundCommitChunkWhenPrimaryRelocated() throws Exception {
        startMasterOnlyNode();
        final var indexNodeA = startIndexNode(
            Settings.builder()
                .put(IndexingDiskController.INDEXING_DISK_INTERVAL_TIME_SETTING.getKey(), TimeValue.timeValueHours(1L))
                .build()
        );
        final var searchNode = startSearchNode();
        var indexName = randomIdentifier();
        createIndex(indexName, 1, 1);
        ensureGreen(indexName);
        indexDocsAndRefresh(indexName, randomIntBetween(1, 5));

        startIndexNode(
            Settings.builder()
                .put(IndexingDiskController.INDEXING_DISK_INTERVAL_TIME_SETTING.getKey(), TimeValue.timeValueHours(1L))
                .build()
        );
        ensureStableCluster(4);

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
                randomNonNegativeInt(),
                randomNonNegativeInt(),
                length,
                ActionListener.wrap(response -> {
                    assert false : "exception expected";
                }, e -> {
                    assertThat(e.getCause(), instanceOf(IndexNotFoundException.class));
                    exceptionThrown.countDown();
                })
            );
        }).start();

        safeAwait(actionSent);
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", indexNodeA), indexName);
        assertBusy(() -> assertThat(internalCluster().nodesInclude(indexName), not(hasItem(indexNodeA))));
        relocationCompleted.countDown();
        safeAwait(exceptionThrown);
    }

    public void testSearchGetVirtualBatchedCompoundCommitChunkRetryConnectivity() throws Exception {
        startMasterOnlyNode();
        final var indexNode = startIndexNode(
            Settings.builder()
                .put(IndexingDiskController.INDEXING_DISK_INTERVAL_TIME_SETTING.getKey(), TimeValue.timeValueHours(1L))
                .build()
        );
        startSearchNode();
        var indexName = randomIdentifier();
        createIndex(indexName, 1, 1);
        ensureGreen(indexName);
        indexDocsAndRefresh(indexName, randomIntBetween(1, 5));

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
                            new NodeClosedException(transportService.getLocalNode())
                        )
                    );
                } else {
                    handler.messageReceived(request, channel, task);
                }
            }
        );

        final var index = resolveIndex(indexName);
        final var searchShard = findSearchShard(index, 0);
        final var searchEngine = getShardEngine(searchShard, SearchEngine.class);
        int length = randomIntBetween(5, 20);
        CountDownLatch validated = new CountDownLatch(1);
        searchEngine.getVirtualBatchedCompoundCommitChunk(
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            length,
            ActionListener.wrap(response -> {
                validateVirtualBatchedCompoundCommitResponse(response, length);
                validated.countDown();
            }, e -> { assert false : e; })
        );
        safeAwait(validated);
    }

    public void testSearchGetVirtualBatchedCompoundCommitChunkFailWithWrongPrimaryTerm() throws Exception {
        startMasterOnlyNode();
        startIndexNode(
            Settings.builder()
                .put(IndexingDiskController.INDEXING_DISK_INTERVAL_TIME_SETTING.getKey(), TimeValue.timeValueHours(1L))
                .build()
        );
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
                randomNonNegativeInt(),
                randomNonNegativeInt(),
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
