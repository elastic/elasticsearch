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

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.stateless.reshard.ReshardIndexRequest;
import org.elasticsearch.xpack.stateless.reshard.TransportReshardAction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;

public class StatelessReshardMixedOperationsIT extends AbstractStatelessPluginIntegTestCase {
    public void testIndexingAndSearchDuringSplit() throws InterruptedException {
        startMasterOnlyNode();

        int shards = randomIntBetween(2, 5);

        int indexNodes = randomIntBetween(1, shards);
        startIndexNodes(indexNodes);
        int searchNodes = randomIntBetween(1, shards);
        startSearchNodes(searchNodes);
        ensureStableCluster(1 + indexNodes + searchNodes);

        String indexName = randomIndexName();
        createIndex(indexName, shards, 1);
        Index index = resolveIndex(indexName);

        int threadsCount = randomIntBetween(1, 10);
        var threads = new ArrayList<Thread>();

        // Let threads run for a bit so that we have some data to move around during split.
        var readyForSplit = new CountDownLatch(threadsCount);
        for (int i = 0; i < threadsCount; i++) {
            int threadIndex = i;
            // We don't need a lot of operations since we'll block both indexing and refresh at some point during split.
            // And as a result most of them will be executed in the later stages of the split which is not that useful here.
            var threadOperations = randomOperations(randomIntBetween(10, 50));
            var thread = new Thread(() -> executeOperations(indexName, threadIndex, threadsCount, threadOperations, readyForSplit));
            thread.start();
            threads.add(thread);
        }

        readyForSplit.await();

        // TODO execute multiple rounds
        int splitRounds = 1;
        for (int i = 0; i < splitRounds; i++) {
            client().execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet();
            awaitClusterState((state) -> state.metadata().projectFor(index).index(indexName).getReshardingMetadata() == null);
        }

        for (int i = 0; i < threadsCount; i++) {
            threads.get(i).join(SAFE_AWAIT_TIMEOUT.millis());
        }
    }

    @Override
    protected Settings.Builder nodeSettings() {
        // Test framework randomly sets this to 0, but we rely on retries to handle target shards still being in recovery
        // when we start re-splitting bulk requests.
        return super.nodeSettings().put(TransportReplicationAction.REPLICATION_RETRY_TIMEOUT.getKey(), "60s");
    }

    private void executeOperations(
        String indexName,
        int threadIndex,
        int threadCount,
        List<Operation> operations,
        CountDownLatch halfwayDone
    ) {
        var indexed = new HashMap<String, String>();
        var indexedAndRefreshed = new HashMap<String, String>();

        // Prevent other threads from updating our documents since then we wouldn't be able to do asserts.
        int id = Integer.MAX_VALUE / threadCount * threadIndex;

        for (int i = 0; i < operations.size(); i++) {
            if (i == operations.size() / 2) {
                halfwayDone.countDown();
            }

            switch (operations.get(i)) {
                case REFRESH -> {
                    var refreshResult = refresh(indexName);
                    assertEquals(0, refreshResult.getFailedShards());

                    indexedAndRefreshed.putAll(indexed);
                    indexed.clear();
                }
                case SEARCH -> {
                    var search = prepareSearch(indexName)
                        // We expect resharding to be seamless.
                        .setAllowPartialSearchResults(false)
                        .setQuery(QueryBuilders.matchAllQuery())
                        .setSize(10000);
                    assertResponse(search, r -> {
                        Map<String, String> fieldValueInHits = Arrays.stream(r.getHits().getHits())
                            .collect(Collectors.toMap(SearchHit::getId, h -> (String) h.getSourceAsMap().get("field")));
                        for (var entry : indexedAndRefreshed.entrySet()) {
                            // Everything we've indexed and refreshed should be present.
                            // Other threads are working too so there will be more but shouldn't be less.
                            var fieldValue = fieldValueInHits.get(entry.getKey());
                            assertEquals(entry.getValue(), fieldValue);
                        }
                    });
                }
                case INDEX -> {
                    // we assume these are less used than bulks
                    boolean useIndexApi = randomDouble() < 0.1;
                    if (useIndexApi) {
                        var indexRequest = createIndexRequest(indexName, id, indexed);
                        id += 1;

                        assertEquals(RestStatus.CREATED, indexRequest.get().status());
                    } else {
                        int bulkSize = randomIntBetween(1, 20);
                        final var client = client();
                        var bulkRequest = client.prepareBulk();
                        for (int j = 0; j < bulkSize; j++) {
                            var indexRequest = createIndexRequest(indexName, id, indexed);
                            id += 1;

                            bulkRequest.add(indexRequest);
                        }
                        var bulkResponse = bulkRequest.get();
                        if (bulkResponse.hasFailures()) {
                            var message = new StringBuilder("Bulk request failed. Failures:\n");
                            for (var response : bulkResponse) {
                                if (response.isFailed()) {
                                    message.append(ExceptionsHelper.unwrapCause(response.getFailure().getCause()));
                                    message.append("\n-----\n");
                                }
                            }
                            throw new AssertionError(message);
                        }
                    }
                }
            }
        }

    }

    private IndexRequestBuilder createIndexRequest(String indexName, int id, Map<String, String> indexed) {
        var indexRequest = client().prepareIndex(indexName);

        String documentId = "document" + id;
        indexRequest.setId(documentId);

        String fieldValue = randomUnicodeOfCodepointLengthBetween(1, 25);
        indexRequest.setSource(Map.of("field", fieldValue));

        indexed.put(documentId, fieldValue);

        if (randomBoolean()) {
            indexRequest.setRouting(randomAlphaOfLength(5));
        }

        return indexRequest;
    }

    // Generates a list of random operations with some basic logic to make it somewhat realistic.
    private List<Operation> randomOperations(int size) {
        var result = new ArrayList<Operation>(size);

        for (int i = 0; i < size; i++) {
            int roll = randomIntBetween(0, 100);
            // Steady state of indexing.
            if (roll < 60) {
                result.add(Operation.INDEX);
            } else if (roll < 70) {
                // 10% percent refresh
                result.add(Operation.REFRESH);
            } else {
                // 30% search
                result.add(Operation.SEARCH);
            }
        }

        return result;
    }

    enum Operation {
        INDEX,
        REFRESH,
        SEARCH
    }
}
