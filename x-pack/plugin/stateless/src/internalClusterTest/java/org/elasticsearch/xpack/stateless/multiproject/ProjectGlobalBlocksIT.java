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

package co.elastic.elasticsearch.stateless.multiproject;

import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.hamcrest.CoreMatchers;

import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

@LuceneTestCase.SuppressFileSystems("*")
public class ProjectGlobalBlocksIT extends AbstractStatelessIntegTestCase {
    @Override
    protected boolean multiProjectIntegrationTest() {
        return true;
    }

    public void testProjectGlobalBlock() throws Exception {
        startMasterAndIndexNode();
        startSearchNode();
        ensureStableCluster(2);
        var projectId = randomUniqueProjectId();
        putProject(projectId);

        var projectClient = internalCluster().client().projectClient(projectId);
        final var indexName = "index-1";
        int numDocs = randomIntBetween(10, 50);
        indexDocsAndRefresh(projectClient, indexName, numDocs);
        // The block should fail any further indexing
        applyProjectGlobalBlock(projectId);
        var bulkRequest2 = projectClient.prepareBulk();
        for (int i = 0; i < numDocs; i++) {
            bulkRequest2.add(new IndexRequest(indexName).source("field", randomUnicodeOfCodepointLengthBetween(1, 25)));
        }
        assertFailedDueToDeletionBlock(safeAwaitFailure(SubscribableListener.<BulkResponse>newForked(bulkRequest2::execute)));
        // searches should also fail
        assertFailedDueToDeletionBlock(
            safeAwaitFailure(SubscribableListener.<SearchResponse>newForked(l -> projectClient.prepareSearch(indexName).execute(l)))
        );
        // Creation of new indices should also fail
        assertFailedDueToDeletionBlock(
            safeAwaitFailure(
                SubscribableListener.<CreateIndexResponse>newForked(
                    l -> projectClient.admin().indices().prepareCreate("index-2").execute(l)
                )
            )
        );
        // remove the block to be able to query the index
        removeAllProjectBlocks(projectId);
        var getIndexException = safeAwaitFailure(
            SubscribableListener.<GetIndexResponse>newForked(
                l -> projectClient.admin().indices().getIndex(new GetIndexRequest(TimeValue.THIRTY_SECONDS).indices("index-2"), l)
            )
        );
        assertThat(getIndexException, CoreMatchers.instanceOf(IndexNotFoundException.class));
        assertHitCount(projectClient.prepareSearch(indexName), numDocs);

        indexDocsAndRefresh(projectClient, indexName, numDocs);
        safeAwait(
            SubscribableListener.<CreateIndexResponse>newForked(l -> projectClient.admin().indices().prepareCreate("index-2").execute(l))
        );

        assertHitCount(projectClient.prepareSearch(indexName), numDocs * 2);
        GetIndexResponse getIndexResponse = safeAwait(
            SubscribableListener.<GetIndexResponse>newForked(
                l -> projectClient.admin().indices().getIndex(new GetIndexRequest(TimeValue.THIRTY_SECONDS).indices("index-2"), l)
            )
        );
        assertThat(getIndexResponse.indices(), CoreMatchers.equalTo(new String[] { "index-2" }));

        removeProject(projectId);
    }

    private void assertFailedDueToDeletionBlock(Exception exception) {
        ClusterBlockException clusterBlockException = (ClusterBlockException) ExceptionsHelper.unwrap(
            exception,
            ClusterBlockException.class
        );
        assertNotNull("expected a ClusterBlockException", clusterBlockException);
        assertTrue(clusterBlockException.blocks().contains(ProjectMetadata.PROJECT_UNDER_DELETION_BLOCK));
    }

    private void applyProjectGlobalBlock(ProjectId projectId) {
        var done = new CountDownLatch(1);
        internalCluster().clusterService(internalCluster().getMasterName())
            .submitUnbatchedStateUpdateTask("add block", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    logger.info("--> adding project global block for project [{}]", projectId);
                    return ClusterState.builder(currentState)
                        .blocks(
                            ClusterBlocks.builder(currentState.blocks())
                                .addProjectGlobalBlock(projectId, ProjectMetadata.PROJECT_UNDER_DELETION_BLOCK)
                                .build()
                        )
                        .build();
                }

                @Override
                public void onFailure(Exception e) {
                    fail(e, "Failed to add project global block for project [" + projectId + "]");
                }

                @Override
                public void clusterStateProcessed(ClusterState initialState, ClusterState newState) {
                    logger.info("--> applied block for project [{}]", projectId);
                    done.countDown();
                }
            });
        safeAwait(done);
    }

    private void removeAllProjectBlocks(ProjectId projectId) {
        var done = new CountDownLatch(1);
        internalCluster().clusterService(internalCluster().getMasterName())
            .submitUnbatchedStateUpdateTask("remove blocks", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    logger.info("--> removing all project blocks for project [{}]", projectId);
                    return ClusterState.builder(currentState)
                        .blocks(ClusterBlocks.builder(currentState.blocks()).removeProject(projectId).build())
                        .build();
                }

                @Override
                public void onFailure(Exception e) {
                    fail(e, "Failed to remove all project blocks for project [" + projectId + "]");
                }

                @Override
                public void clusterStateProcessed(ClusterState initialState, ClusterState newState) {
                    logger.info("--> removed all blocks for project [{}]", projectId);
                    done.countDown();
                }
            });
        safeAwait(done);
    }
}
