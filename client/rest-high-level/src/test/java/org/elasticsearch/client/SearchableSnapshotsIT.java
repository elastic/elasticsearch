/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.searchable_snapshots.MountSnapshotRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.is;

public class SearchableSnapshotsIT extends ESRestHighLevelClientTestCase {

    public void testMountSnapshot() throws IOException {
        {
            final CreateIndexRequest request = new CreateIndexRequest("index");
            final CreateIndexResponse response = highLevelClient().indices().create(request, RequestOptions.DEFAULT);
            assertThat(response.isAcknowledged(), is(true));
        }

        {
            final IndexRequest request = new IndexRequest("index")
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .source("{}", XContentType.JSON);
            final IndexResponse response = highLevelClient().index(request, RequestOptions.DEFAULT);
            assertThat(response.status(), is(RestStatus.CREATED));
        }

        {
            final PutRepositoryRequest request = new PutRepositoryRequest("repository");
            request.settings("{\"location\": \".\"}", XContentType.JSON);
            request.type(FsRepository.TYPE);
            final AcknowledgedResponse response = highLevelClient().snapshot().createRepository(request, RequestOptions.DEFAULT);
            assertThat(response.isAcknowledged(), is(true));
        }

        {
            final CreateSnapshotRequest request =
                new CreateSnapshotRequest("repository", "snapshot").waitForCompletion(true);
            final CreateSnapshotResponse response = highLevelClient().snapshot().create(request, RequestOptions.DEFAULT);
            assertThat(response.getSnapshotInfo().status(), is(RestStatus.OK));
        }

        {
            final MountSnapshotRequest request = new MountSnapshotRequest("repository", "snapshot", "index")
                .waitForCompletion(true)
                .renamedIndex("renamed_index");
            final SearchableSnapshotsClient client = new SearchableSnapshotsClient(highLevelClient());
            final RestoreSnapshotResponse response = execute(request, client::mountSnapshot, client::mountSnapshotAsync);
            assertThat(response.getRestoreInfo().successfulShards(), is(1));
        }

        {
            final SearchRequest request = new SearchRequest("renamed_index");
            final SearchResponse response = highLevelClient().search(request, RequestOptions.DEFAULT);
            assertThat(response.getHits().getTotalHits().value, is(1L));
            assertThat(response.getHits().getHits()[0].getSourceAsMap(), anEmptyMap());
        }
    }

}
