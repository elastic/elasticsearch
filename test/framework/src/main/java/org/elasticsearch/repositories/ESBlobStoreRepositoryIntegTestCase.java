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
package org.elasticsearch.repositories;

import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequestBuilder;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequestBuilder;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Basic integration tests for blob-based repository validation.
 */
public abstract class ESBlobStoreRepositoryIntegTestCase extends ESIntegTestCase {

    protected abstract void createTestRepository(String name);

    public void testSnapshotAndRestore() throws Exception {
        String repoName = randomAsciiName();
        logger.info("-->  creating repository {}", repoName);
        createTestRepository(repoName);
        int indexCount = randomIntBetween(1, 5);
        int[] docCounts = new int[indexCount];
        String[] indexNames = generateRandomNames(indexCount);
        for (int i = 0; i < indexCount; i++) {
            logger.info("-->  create random index {} with {} records", indexNames[i], docCounts[i]);
            docCounts[i] = iterations(10, 1000);
            addRandomDocuments(indexNames[i], docCounts[i]);
            assertHitCount(client().prepareSearch(indexNames[i]).setSize(0).get(), docCounts[i]);
        }

        String snapshotName = randomAsciiName();
        logger.info("-->  create snapshot {}:{}", repoName, snapshotName);
        assertSuccessfulSnapshot(client().admin().cluster().prepareCreateSnapshot(repoName, snapshotName)
                .setWaitForCompletion(true).setIndices(indexNames));

        List<String> deleteIndices = randomSubsetOf(randomIntBetween(0, indexCount), indexNames);
        if (deleteIndices.size() > 0) {
            logger.info("-->  delete indices {}", deleteIndices);
            assertAcked(client().admin().indices().prepareDelete(deleteIndices.toArray(new String[deleteIndices.size()])));
        }

        Set<String> closeIndices = new HashSet<>(Arrays.asList(indexNames));
        closeIndices.removeAll(deleteIndices);

        if (closeIndices.size() > 0) {
            for (String index : closeIndices) {
                if (randomBoolean()) {
                    logger.info("--> add random documents to {}", index);
                    addRandomDocuments(index, randomIntBetween(10, 1000));
                } else {
                    int docCount = (int) client().prepareSearch(index).setSize(0).get().getHits().totalHits();
                    int deleteCount = randomIntBetween(1, docCount);
                    logger.info("--> delete {} random documents from {}", deleteCount, index);
                    for (int i = 0; i < deleteCount; i++) {
                        int doc = randomIntBetween(0, docCount - 1);
                        client().prepareDelete(index, index, Integer.toString(doc)).get();
                    }
                    client().admin().indices().prepareRefresh(index).get();
                }
            }

            logger.info("-->  close indices {}", closeIndices);
            assertAcked(client().admin().indices().prepareClose(closeIndices.toArray(new String[closeIndices.size()])));
        }

        logger.info("--> restore all indices from the snapshot");
        assertSuccessfulRestore(client().admin().cluster().prepareRestoreSnapshot(repoName, snapshotName).setWaitForCompletion(true));

        ensureGreen();

        for (int i = 0; i < indexCount; i++) {
            assertHitCount(client().prepareSearch(indexNames[i]).setSize(0).get(), docCounts[i]);
        }

        logger.info("-->  delete snapshot {}:{}", repoName, snapshotName);
        assertAcked(client().admin().cluster().prepareDeleteSnapshot(repoName, snapshotName).get());
    }

    public void testMultipleSnapshotAndRollback() throws Exception {
        String repoName = randomAsciiName();
        logger.info("-->  creating repository {}", repoName);
        createTestRepository(repoName);
        int iterationCount = randomIntBetween(2, 5);
        int[] docCounts = new int[iterationCount];
        String indexName = randomAsciiName();
        String snapshotName = randomAsciiName();
        assertAcked(client().admin().indices().prepareCreate(indexName).get());
        for (int i = 0; i < iterationCount; i++) {
            if (randomBoolean() && i > 0) { // don't delete on the first iteration
                int docCount = docCounts[i - 1];
                if (docCount > 0) {
                    int deleteCount = randomIntBetween(1, docCount);
                    logger.info("--> delete {} random documents from {}", deleteCount, indexName);
                    for (int j = 0; j < deleteCount; j++) {
                        int doc = randomIntBetween(0, docCount - 1);
                        client().prepareDelete(indexName, indexName, Integer.toString(doc)).get();
                    }
                    client().admin().indices().prepareRefresh(indexName).get();
                }
            } else {
                int docCount = randomIntBetween(10, 1000);
                logger.info("--> add {} random documents to {}", docCount, indexName);
                addRandomDocuments(indexName, docCount);
            }
            // Check number of documents in this iteration
            docCounts[i] = (int) client().prepareSearch(indexName).setSize(0).get().getHits().totalHits();
            logger.info("-->  create snapshot {}:{} with {} documents", repoName, snapshotName + "-" + i, docCounts[i]);
            assertSuccessfulSnapshot(client().admin().cluster().prepareCreateSnapshot(repoName, snapshotName + "-" + i)
                    .setWaitForCompletion(true).setIndices(indexName));
        }

        int restoreOperations = randomIntBetween(1, 3);
        for (int i = 0; i < restoreOperations; i++) {
            int iterationToRestore = randomIntBetween(0, iterationCount - 1);
            logger.info("-->  performing restore of the iteration {}", iterationToRestore);

            logger.info("-->  close index");
            assertAcked(client().admin().indices().prepareClose(indexName));

            logger.info("--> restore index from the snapshot");
            assertSuccessfulRestore(client().admin().cluster().prepareRestoreSnapshot(repoName, snapshotName + "-" + iterationToRestore)
                    .setWaitForCompletion(true));

            ensureGreen();
            assertHitCount(client().prepareSearch(indexName).setSize(0).get(), docCounts[iterationToRestore]);
        }

        for (int i = 0; i < iterationCount; i++) {
            logger.info("-->  delete snapshot {}:{}", repoName, snapshotName + "-" + i);
            assertAcked(client().admin().cluster().prepareDeleteSnapshot(repoName, snapshotName + "-" + i).get());
        }
    }

    protected void addRandomDocuments(String name, int numDocs) throws ExecutionException, InterruptedException {
        IndexRequestBuilder[] indexRequestBuilders = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            indexRequestBuilders[i] = client().prepareIndex(name, name, Integer.toString(i))
                .setRouting(randomAsciiOfLength(randomIntBetween(1, 10))).setSource("field", "value");
        }
        indexRandom(true, indexRequestBuilders);
    }

    protected String[] generateRandomNames(int num) {
        Set<String> names = new HashSet<>();
        for (int i = 0; i < num; i++) {
            String name;
            do {
                name = randomAsciiName();
            } while (names.contains(name));
            names.add(name);
        }
        return names.toArray(new String[num]);
    }

    public static CreateSnapshotResponse assertSuccessfulSnapshot(CreateSnapshotRequestBuilder requestBuilder) {
        CreateSnapshotResponse response = requestBuilder.get();
        assertSuccessfulSnapshot(response);
        return response;
    }

    public static void assertSuccessfulSnapshot(CreateSnapshotResponse response) {
        assertThat(response.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(response.getSnapshotInfo().successfulShards(), equalTo(response.getSnapshotInfo().totalShards()));
    }

    public static RestoreSnapshotResponse assertSuccessfulRestore(RestoreSnapshotRequestBuilder requestBuilder) {
        RestoreSnapshotResponse response = requestBuilder.get();
        assertSuccessfulRestore(response);
        return response;
    }

    public static void assertSuccessfulRestore(RestoreSnapshotResponse response) {
        assertThat(response.getRestoreInfo().successfulShards(), greaterThan(0));
        assertThat(response.getRestoreInfo().successfulShards(), equalTo(response.getRestoreInfo().totalShards()));
    }

    public static String randomAsciiName() {
        return randomAsciiOfLength(randomIntBetween(1, 10)).toLowerCase(Locale.ROOT);
    }
}
