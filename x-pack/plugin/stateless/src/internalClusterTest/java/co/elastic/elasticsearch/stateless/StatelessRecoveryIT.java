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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.junit.Before;

import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class StatelessRecoveryIT extends AbstractStatelessIntegTestCase {

    private final int numShards = randomIntBetween(1, 3);

    @Before
    public void init() {
        startMasterOnlyNode();
        startIndexNodes(numShards);
    }

    public void testRelocatingIndexShards() throws Exception {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
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
            final Set<String> existingNodes = new HashSet<>(internalCluster().nodesInclude(indexName));
            if (existingNodes.size() == 1) {
                startIndexNode();
            }

            final String stoppedNode = randomFrom(existingNodes);
            updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", stoppedNode), indexName);
            ensureGreen(TimeValue.timeValueSeconds(30L), indexName);
            running.set(false);
            for (Thread thread : threads) {
                thread.join();
            }
            internalCluster().stopNode(stoppedNode);
            ensureGreen(indexName);
        }
    }

}
