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

package co.elastic.elasticsearch.stateless;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.junit.Before;

import java.io.IOException;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

public class StatelessRecoveryIT extends AbstractStatelessIntegTestCase {

    @Before
    public void init() {
        startMasterOnlyNode();
    }

    public void testRelocatingIndexShards() throws Exception {
        var numShards = randomIntBetween(1, 3);
        startIndexNodes(numShards);

        final String indexName = randomIdentifier();
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), false)
                .build()
        );
        ensureGreen(indexName);
        indexDocs(indexName, randomIntBetween(1, 100));
        if (randomBoolean()) {
            flush(indexName);
        }

        final int iters = randomIntBetween(5, 10);
        for (int i = 0; i < iters; i++) {
            final AtomicBoolean running = new AtomicBoolean(true);

            final Thread[] threads = new Thread[scaledRandomIntBetween(1, 3)];
            for (int j = 0; j < threads.length; j++) {
                threads[j] = new Thread(() -> {
                    while (running.get()) {
                        indexDocs(indexName, 20);
                    }
                });
                threads[j].start();
            }
            var initialPrimaryTerms = getPrimaryTerms(indexName);
            var existingNodes = new HashSet<>(internalCluster().nodesInclude(indexName));
            startIndexNode();

            final String nodeToRemove = randomFrom(existingNodes);
            updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", nodeToRemove), indexName);

            assertBusy(() -> assertThat(internalCluster().nodesInclude(indexName), not(hasItem(nodeToRemove))));
            ensureGreen(indexName);

            running.set(false);
            for (Thread thread : threads) {
                thread.join();
            }

            var updatedPrimaryTerms = getPrimaryTerms(indexName);
            assertPrimaryTermIncremented(initialPrimaryTerms, updatedPrimaryTerms);

            internalCluster().stopNode(nodeToRemove);
            ensureGreen(indexName);
        }
    }

    private long[] getPrimaryTerms(String indexName) {
        var response = client().admin().cluster().prepareState().get();
        var state = response.getState();

        var indexMetadata = state.metadata().index(indexName);
        long[] primaryTerms = new long[indexMetadata.getNumberOfShards()];
        for (int i = 0; i < primaryTerms.length; i++) {
            primaryTerms[i] = indexMetadata.primaryTerm(i);
        }
        return primaryTerms;
    }

    private void assertPrimaryTermIncremented(long[] initial, long[] updated) {
        assertThat(updated.length, equalTo(initial.length));
        long totalInitial = 0;
        long totalUpdated = 0;
        for (int i = 0; i < initial.length; i++) {
            totalInitial += initial[i];
            totalUpdated += updated[i];
            assertThat(updated[i], anyOf(equalTo(initial[i]), equalTo(initial[i] + 1)));
        }
        assertThat("At least 1 primary term should increment", totalUpdated, equalTo(totalInitial + 1));
    }

    public void testRecoverIndexingShard() throws Exception {

        var indexingNode1 = startIndexNode();
        startSearchNode();

        var indexName = randomIdentifier();
        createIndex(indexName, 1, 1);
        ensureGreen(indexName);

        int numDocsRound1 = randomIntBetween(1, 100);
        indexDocs(indexName, numDocsRound1);
        refresh(indexName);

        assertHitCount(client().prepareSearch(indexName).get(), numDocsRound1);

        if (randomBoolean()) {
            internalCluster().restartNode(indexingNode1);
        } else {
            internalCluster().stopNode(indexingNode1);
            startIndexNode(); // replacement node
        }

        ensureGreen(indexName);

        int numDocsRound2 = randomIntBetween(1, 100);
        indexDocs(indexName, numDocsRound2);
        refresh(indexName);
        assertHitCount(client().prepareSearch(indexName).get(), numDocsRound1 + numDocsRound2);
    }

    public void testRecoverSearchShard() throws IOException {

        startIndexNode();

        var indexName = randomIdentifier();
        createIndex(
            indexName,
            Settings.builder() //
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1) //
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0) //
                .build()
        );
        ensureGreen(indexName);

        int numDocs = randomIntBetween(1, 100);
        indexDocs(indexName, numDocs);
        refresh(indexName);

        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1));

        var searchNode1 = startSearchNode();
        ensureGreen(indexName);
        assertHitCount(client().prepareSearch(indexName).get(), numDocs);
        internalCluster().stopNode(searchNode1);

        var searchNode2 = startSearchNode();
        ensureGreen(indexName);
        assertHitCount(client().prepareSearch(indexName).get(), numDocs);
        internalCluster().stopNode(searchNode2);
    }
}
