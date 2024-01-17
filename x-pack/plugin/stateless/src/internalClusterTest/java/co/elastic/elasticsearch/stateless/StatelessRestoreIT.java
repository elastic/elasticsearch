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

import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.snapshots.SnapshotRestoreException;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class StatelessRestoreIT extends AbstractStatelessIntegTestCase {

    @Before
    public void init() {
        startMasterOnlyNode();
    }

    public void testRestoreShardOverClosedIndex() {
        startIndexNodes(1);
        startSearchNode();
        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);

        int numDocs = randomIntBetween(1, 1000);
        indexDocs(indexName, numDocs);
        flush(indexName);

        createRepository(logger, "test-repo", "fs");
        logger.info("--> creating snapshot [{}] of {} in [{}]", "test-snap", Collections.singletonList(indexName), "test-repo");
        createSnapshot("test-repo", "test-snap", Collections.singletonList(indexName));

        assertAcked(indicesAdmin().prepareClose(indexName));

        final RestoreSnapshotResponse restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(true)
            .get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().successfulShards(), equalTo(1));
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));
        ensureGreen(indexName);

        assertDocCount(indexName, numDocs);
    }

    public void testRenameOnRestore() throws Exception {
        startIndexNodes(1);
        startSearchNode();
        Client client = client();

        createRepository(logger, "test-repo", "fs");

        createIndex("test-idx-1", indexSettings(1, 1).build());
        createIndex("test-idx-2", indexSettings(1, 1).build());
        createIndex("test-idx-3", indexSettings(1, 1).build());
        ensureGreen("test-idx-1", "test-idx-2", "test-idx-3");

        assertAcked(
            client.admin()
                .indices()
                .prepareAliases()
                .addAlias("test-idx-1", "alias-1", false)
                .addAlias("test-idx-2", "alias-2", false)
                .addAlias("test-idx-3", "alias-3", false)
        );

        indexDocs("test-idx-1", 100);
        indexDocs("test-idx-2", 100);
        flush("test-idx-1");
        flush("test-idx-2");

        createSnapshot("test-repo", "test-snap", Arrays.asList("test-idx-1", "test-idx-2"));

        logger.info("--> restore indices with different names");
        RestoreSnapshotResponse restoreSnapshotResponse = client.admin()
            .cluster()
            .prepareRestoreSnapshot("test-repo", "test-snap")
            .setRenamePattern("(.+)")
            .setRenameReplacement("$1-copy")
            .setWaitForCompletion(true)
            .execute()
            .actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));
        ensureGreen("test-idx-1-copy", "test-idx-2-copy");

        assertDocCount("test-idx-1-copy", 100L);
        assertDocCount("test-idx-2-copy", 100L);

        logger.info("--> close just restored indices");
        client.admin().indices().prepareClose("test-idx-1-copy", "test-idx-2-copy").get();

        logger.info("--> and try to restore these indices again");
        restoreSnapshotResponse = client.admin()
            .cluster()
            .prepareRestoreSnapshot("test-repo", "test-snap")
            .setRenamePattern("(.+)")
            .setRenameReplacement("$1-copy")
            .setWaitForCompletion(true)
            .execute()
            .actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));
        ensureGreen("test-idx-1-copy", "test-idx-2-copy");

        assertDocCount("test-idx-1-copy", 100L);
        assertDocCount("test-idx-2-copy", 100L);

        logger.info("--> close indices");
        assertAcked(client.admin().indices().prepareClose("test-idx-1", "test-idx-2-copy"));

        logger.info("--> restore indices with different names");
        restoreSnapshotResponse = client.admin()
            .cluster()
            .prepareRestoreSnapshot("test-repo", "test-snap")
            .setRenamePattern("(.+-2)")
            .setRenameReplacement("$1-copy")
            .setWaitForCompletion(true)
            .execute()
            .actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        logger.info("--> delete indices");
        cluster().wipeIndices("test-idx-1", "test-idx-1-copy", "test-idx-2", "test-idx-2-copy");

        logger.info("--> try renaming indices using the same name");
        try {
            client.admin()
                .cluster()
                .prepareRestoreSnapshot("test-repo", "test-snap")
                .setRenamePattern("(.+)")
                .setRenameReplacement("same-name")
                .setWaitForCompletion(true)
                .execute()
                .actionGet();
            fail("Shouldn't be here");
        } catch (SnapshotRestoreException ex) {
            // Expected
        }

        logger.info("--> try renaming indices using the same name");
        try {
            client.admin()
                .cluster()
                .prepareRestoreSnapshot("test-repo", "test-snap")
                .setRenamePattern("test-idx-2")
                .setRenameReplacement("test-idx-1")
                .setWaitForCompletion(true)
                .execute()
                .actionGet();
            fail("Shouldn't be here");
        } catch (SnapshotRestoreException ex) {
            // Expected
        }

        logger.info("--> try renaming indices using invalid index name");
        try {
            client.admin()
                .cluster()
                .prepareRestoreSnapshot("test-repo", "test-snap")
                .setIndices("test-idx-1")
                .setRenamePattern(".+")
                .setRenameReplacement("__WRONG__")
                .setWaitForCompletion(true)
                .execute()
                .actionGet();
            fail("Shouldn't be here");
        } catch (InvalidIndexNameException ex) {
            // Expected
        }

        logger.info("--> try renaming indices into existing alias name");
        try {
            client.admin()
                .cluster()
                .prepareRestoreSnapshot("test-repo", "test-snap")
                .setIndices("test-idx-1")
                .setRenamePattern(".+")
                .setRenameReplacement("alias-3")
                .setWaitForCompletion(true)
                .execute()
                .actionGet();
            fail("Shouldn't be here");
        } catch (InvalidIndexNameException ex) {
            // Expected
        }

        logger.info("--> try renaming indices into existing alias of itself");
        try {
            client.admin()
                .cluster()
                .prepareRestoreSnapshot("test-repo", "test-snap")
                .setIndices("test-idx-1")
                .setRenamePattern("test-idx")
                .setRenameReplacement("alias")
                .setWaitForCompletion(true)
                .execute()
                .actionGet();
            fail("Shouldn't be here");
        } catch (SnapshotRestoreException ex) {
            // Expected
        }

        logger.info("--> try renaming indices into existing alias of another restored index");
        try {
            client.admin()
                .cluster()
                .prepareRestoreSnapshot("test-repo", "test-snap")
                .setIndices("test-idx-1", "test-idx-2")
                .setRenamePattern("test-idx-1")
                .setRenameReplacement("alias-2")
                .setWaitForCompletion(true)
                .execute()
                .actionGet();
            fail("Shouldn't be here");
        } catch (SnapshotRestoreException ex) {
            // Expected
        }

        logger.info("--> try renaming indices into existing alias of itself, but don't restore aliases ");
        restoreSnapshotResponse = client.admin()
            .cluster()
            .prepareRestoreSnapshot("test-repo", "test-snap")
            .setIndices("test-idx-1")
            .setRenamePattern("test-idx")
            .setRenameReplacement("alias")
            .setWaitForCompletion(true)
            .setIncludeAliases(false)
            .execute()
            .actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));
    }

    public void testRestoreIncreasesPrimaryTerms() {
        startIndexNodes(1);
        startSearchNode();
        final String indexName = randomIdentifier();
        createIndex(indexName, 2, 0);
        ensureGreen(indexName);

        if (randomBoolean()) {
            // open and close the index to increase the primary terms
            for (int i = 0; i < randomInt(3); i++) {
                assertAcked(indicesAdmin().prepareClose(indexName));
                assertAcked(indicesAdmin().prepareOpen(indexName));
            }
        }

        final IndexMetadata indexMetadata = clusterAdmin().prepareState()
            .clear()
            .setIndices(indexName)
            .setMetadata(true)
            .get()
            .getState()
            .metadata()
            .index(indexName);
        assertThat(indexMetadata.getSettings().get(IndexMetadata.SETTING_HISTORY_UUID), nullValue());
        final int numPrimaries = getNumShards(indexName).numPrimaries;
        final Map<Integer, Long> primaryTerms = IntStream.range(0, numPrimaries)
            .boxed()
            .collect(Collectors.toMap(shardId -> shardId, indexMetadata::primaryTerm));

        createRepository(logger, "test-repo", "fs");
        createSnapshot("test-repo", "test-snap", Collections.singletonList(indexName));

        assertAcked(indicesAdmin().prepareClose(indexName));

        final RestoreSnapshotResponse restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(true)
            .get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().successfulShards(), equalTo(numPrimaries));
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));
        ensureGreen(indexName);

        final IndexMetadata restoredIndexMetadata = clusterAdmin().prepareState()
            .clear()
            .setIndices(indexName)
            .setMetadata(true)
            .get()
            .getState()
            .metadata()
            .index(indexName);
        for (int shardId = 0; shardId < numPrimaries; shardId++) {
            assertThat(restoredIndexMetadata.primaryTerm(shardId), greaterThan(primaryTerms.get(shardId)));
        }
        assertThat(restoredIndexMetadata.getSettings().get(IndexMetadata.SETTING_HISTORY_UUID), notNullValue());
    }

    private static void createSnapshot(String repositoryName, String snapshot, List<String> indices) {
        final CreateSnapshotResponse response = clusterAdmin().prepareCreateSnapshot(repositoryName, snapshot)
            .setIndices(indices.toArray(Strings.EMPTY_ARRAY))
            .setWaitForCompletion(true)
            .get();
    }

    private static void assertDocCount(String indexName, long numDocs) {
        assertResponse(prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()), searchResponse -> {
            assertNoFailures(searchResponse);
            assertEquals(numDocs, searchResponse.getHits().getTotalHits().value);
        });
    }
}
