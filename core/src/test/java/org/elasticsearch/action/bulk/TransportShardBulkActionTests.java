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

package org.elasticsearch.action.bulk;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.support.replication.ReplicationOperation;
import org.elasticsearch.action.update.UpdateHelper;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.action.bulk.MappingUpdatePerformer;
import org.elasticsearch.action.bulk.BulkItemResultHolder;

import java.io.IOException;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.containsString;

public class TransportShardBulkActionTests extends IndexShardTestCase {

    private final ShardId shardId = new ShardId("index", "_na_", 0);
    private final Settings idxSettings = Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put("index.version.created", Version.CURRENT.id)
            .build();

    private IndexMetaData indexMetaData() throws IOException {
        return IndexMetaData.builder("index")
                .putMapping("type", "{\"properties\": {\"foo\": {\"type\": \"text\"}}}")
                .settings(idxSettings)
                .primaryTerm(0, 1).build();
    }

    public void testShouldExecuteReplicaItem() throws Exception {
        // Successful index request should be replicated
        DocWriteRequest writeRequest = new IndexRequest("index", "type", "id")
                .source(Requests.INDEX_CONTENT_TYPE, "foo", "bar");
        DocWriteResponse response = new IndexResponse(shardId, "type", "id", 1, 1, randomBoolean());
        BulkItemRequest request = new BulkItemRequest(0, writeRequest);
        request.setPrimaryResponse(new BulkItemResponse(0, DocWriteRequest.OpType.INDEX, response));
        assertTrue(TransportShardBulkAction.shouldExecuteReplicaItem(request, 0));

        // Failed index requests should not be replicated (for now!)
        writeRequest = new IndexRequest("index", "type", "id")
                .source(Requests.INDEX_CONTENT_TYPE, "foo", "bar");
        response = new IndexResponse(shardId, "type", "id", 1, 1, randomBoolean());
        request = new BulkItemRequest(0, writeRequest);
        request.setPrimaryResponse(
                new BulkItemResponse(0, DocWriteRequest.OpType.INDEX,
                        new BulkItemResponse.Failure("test", "type", "id",
                                                     new IllegalArgumentException("i died"))));
        assertFalse(TransportShardBulkAction.shouldExecuteReplicaItem(request, 0));

        // NOOP requests should not be replicated
        writeRequest = new UpdateRequest("index", "type", "id");
        response = new UpdateResponse(shardId, "type", "id", 1, DocWriteResponse.Result.NOOP);
        request = new BulkItemRequest(0, writeRequest);
        request.setPrimaryResponse(new BulkItemResponse(0, DocWriteRequest.OpType.UPDATE,
                        response));
        assertFalse(TransportShardBulkAction.shouldExecuteReplicaItem(request, 0));
    }


    public void testExecuteBulkIndexRequest() throws Exception {
        IndexMetaData metaData = indexMetaData();
        IndexShard shard = newStartedShard(true);

        BulkItemRequest[] items = new BulkItemRequest[1];
        boolean create = randomBoolean();
        DocWriteRequest writeRequest = new IndexRequest("index", "type", "id")
                .source(Requests.INDEX_CONTENT_TYPE, "foo", "bar")
                .create(create);
        BulkItemRequest primaryRequest = new BulkItemRequest(0, writeRequest);
        items[0] = primaryRequest;
        BulkShardRequest bulkShardRequest =
                new BulkShardRequest(shardId, RefreshPolicy.NONE, items);

        Translog.Location location = new Translog.Location(0, 0, 0);
        UpdateHelper updateHelper = null;

        Translog.Location newLocation = TransportShardBulkAction.executeBulkItemRequest(metaData,
                shard, bulkShardRequest, location, 0, updateHelper,
                threadPool::absoluteTimeInMillis, new NoopMappingUpdatePerformer());

        // Translog should change, since there were no problems
        assertThat(newLocation, not(location));

        BulkItemResponse primaryResponse = bulkShardRequest.items()[0].getPrimaryResponse();

        assertThat(primaryResponse.getItemId(), equalTo(0));
        assertThat(primaryResponse.getId(), equalTo("id"));
        assertThat(primaryResponse.getOpType(),
                equalTo(create ? DocWriteRequest.OpType.CREATE : DocWriteRequest.OpType.INDEX));
        assertFalse(primaryResponse.isFailed());

        // Assert that the document actually made it there
        assertDocCount(shard, 1);

        writeRequest = new IndexRequest("index", "type", "id")
                .source(Requests.INDEX_CONTENT_TYPE, "foo", "bar")
                .create(true);
        primaryRequest = new BulkItemRequest(0, writeRequest);
        items[0] = primaryRequest;
        bulkShardRequest = new BulkShardRequest(shardId, RefreshPolicy.NONE, items);

        Translog.Location secondLocation =
                TransportShardBulkAction.executeBulkItemRequest( metaData,
                        shard, bulkShardRequest, newLocation, 0, updateHelper,
                        threadPool::absoluteTimeInMillis, new NoopMappingUpdatePerformer());

        // Translog should not change, since the document was not indexed due to a version conflict
        assertThat(secondLocation, equalTo(newLocation));

        BulkItemRequest replicaRequest = bulkShardRequest.items()[0];

        primaryResponse = bulkShardRequest.items()[0].getPrimaryResponse();

        assertThat(primaryResponse.getItemId(), equalTo(0));
        assertThat(primaryResponse.getId(), equalTo("id"));
        assertThat(primaryResponse.getOpType(), equalTo(DocWriteRequest.OpType.CREATE));
        // Should be failed since the document already exists
        assertTrue(primaryResponse.isFailed());

        BulkItemResponse.Failure failure = primaryResponse.getFailure();
        assertThat(failure.getIndex(), equalTo("index"));
        assertThat(failure.getType(), equalTo("type"));
        assertThat(failure.getId(), equalTo("id"));
        assertThat(failure.getCause().getClass(), equalTo(VersionConflictEngineException.class));
        assertThat(failure.getCause().getMessage(),
                containsString("version conflict, document already exists (current version [1])"));
        assertThat(failure.getStatus(), equalTo(RestStatus.CONFLICT));

        assertThat(replicaRequest, equalTo(primaryRequest));

        // Assert that the document count is still 1
        assertDocCount(shard, 1);
        closeShards(shard);
    }

    public void testExecuteBulkIndexRequestWithRejection() throws Exception {
        IndexMetaData metaData = indexMetaData();
        IndexShard shard = newStartedShard(true);

        BulkItemRequest[] items = new BulkItemRequest[1];
        DocWriteRequest writeRequest = new IndexRequest("index", "type", "id")
                .source(Requests.INDEX_CONTENT_TYPE, "foo", "bar");
        items[0] = new BulkItemRequest(0, writeRequest);
        BulkShardRequest bulkShardRequest =
                new BulkShardRequest(shardId, RefreshPolicy.NONE, items);

        Translog.Location location = new Translog.Location(0, 0, 0);
        UpdateHelper updateHelper = null;

        // Pretend the mappings haven't made it to the node yet, and throw  a rejection
        Exception err = new ReplicationOperation.RetryOnPrimaryException(shardId, "rejection");

        try {
            TransportShardBulkAction.executeBulkItemRequest(metaData, shard, bulkShardRequest,
                    location, 0, updateHelper, threadPool::absoluteTimeInMillis,
                    new ThrowingMappingUpdatePerformer(err));
            fail("should have thrown a retry exception");
        } catch (ReplicationOperation.RetryOnPrimaryException e) {
            assertThat(e, equalTo(err));
        }

        closeShards(shard);
    }

    public void testExecuteBulkIndexRequestWithConflictingMappings() throws Exception {
        IndexMetaData metaData = indexMetaData();
        IndexShard shard = newStartedShard(true);

        BulkItemRequest[] items = new BulkItemRequest[1];
        DocWriteRequest writeRequest = new IndexRequest("index", "type", "id")
                .source(Requests.INDEX_CONTENT_TYPE, "foo", "bar");
        items[0] = new BulkItemRequest(0, writeRequest);
        BulkShardRequest bulkShardRequest =
                new BulkShardRequest(shardId, RefreshPolicy.NONE, items);

        Translog.Location location = new Translog.Location(0, 0, 0);
        UpdateHelper updateHelper = null;

        // Return a mapping conflict (IAE) when trying to update the mapping
        Exception err = new IllegalArgumentException("mapping conflict");

        Translog.Location newLocation = TransportShardBulkAction.executeBulkItemRequest(metaData,
                shard, bulkShardRequest, location, 0, updateHelper,
                threadPool::absoluteTimeInMillis, new ThrowingMappingUpdatePerformer(err));

        // Translog shouldn't change, as there were conflicting mappings
        assertThat(newLocation, equalTo(location));

        BulkItemResponse primaryResponse = bulkShardRequest.items()[0].getPrimaryResponse();

        // Since this was not a conflict failure, the primary response
        // should be filled out with the failure information
        assertThat(primaryResponse.getItemId(), equalTo(0));
        assertThat(primaryResponse.getId(), equalTo("id"));
        assertThat(primaryResponse.getOpType(), equalTo(DocWriteRequest.OpType.INDEX));
        assertTrue(primaryResponse.isFailed());
        assertThat(primaryResponse.getFailureMessage(), containsString("mapping conflict"));
        BulkItemResponse.Failure failure = primaryResponse.getFailure();
        assertThat(failure.getIndex(), equalTo("index"));
        assertThat(failure.getType(), equalTo("type"));
        assertThat(failure.getId(), equalTo("id"));
        assertThat(failure.getCause(), equalTo(err));
        assertThat(failure.getStatus(), equalTo(RestStatus.BAD_REQUEST));

        closeShards(shard);
    }

    public void testExecuteBulkDeleteRequest() throws Exception {
        IndexMetaData metaData = indexMetaData();
        IndexShard shard = newStartedShard(true);

        BulkItemRequest[] items = new BulkItemRequest[1];
        DocWriteRequest writeRequest = new DeleteRequest("index", "type", "id");
        items[0] = new BulkItemRequest(0, writeRequest);
        BulkShardRequest bulkShardRequest =
                new BulkShardRequest(shardId, RefreshPolicy.NONE, items);

        Translog.Location location = new Translog.Location(0, 0, 0);
        UpdateHelper updateHelper = null;

        Translog.Location newLocation = TransportShardBulkAction.executeBulkItemRequest(metaData,
                shard, bulkShardRequest, location, 0, updateHelper,
                threadPool::absoluteTimeInMillis, new NoopMappingUpdatePerformer());

        // Translog changes, even though the document didn't exist
        assertThat(newLocation, not(location));

        BulkItemRequest replicaRequest = bulkShardRequest.items()[0];
        DocWriteRequest replicaDeleteRequest = replicaRequest.request();
        BulkItemResponse primaryResponse = replicaRequest.getPrimaryResponse();
        DeleteResponse response = primaryResponse.getResponse();

        // Any version can be matched on replica
        assertThat(replicaDeleteRequest.version(), equalTo(Versions.MATCH_ANY));
        assertThat(replicaDeleteRequest.versionType(), equalTo(VersionType.INTERNAL));

        assertThat(primaryResponse.getItemId(), equalTo(0));
        assertThat(primaryResponse.getId(), equalTo("id"));
        assertThat(primaryResponse.getOpType(), equalTo(DocWriteRequest.OpType.DELETE));
        assertFalse(primaryResponse.isFailed());

        assertThat(response.getResult(), equalTo(DocWriteResponse.Result.NOT_FOUND));
        assertThat(response.getShardId(), equalTo(shard.shardId()));
        assertThat(response.getIndex(), equalTo("index"));
        assertThat(response.getType(), equalTo("type"));
        assertThat(response.getId(), equalTo("id"));
        assertThat(response.getVersion(), equalTo(1L));
        assertThat(response.getSeqNo(), equalTo(0L));
        assertThat(response.forcedRefresh(), equalTo(false));

        // Now do the same after indexing the document, it should now find and delete the document
        indexDoc(shard, "type", "id", "{\"foo\": \"bar\"}");

        writeRequest = new DeleteRequest("index", "type", "id");
        items[0] = new BulkItemRequest(0, writeRequest);
        bulkShardRequest = new BulkShardRequest(shardId, RefreshPolicy.NONE, items);

        location = newLocation;

        newLocation = TransportShardBulkAction.executeBulkItemRequest(metaData, shard,
                bulkShardRequest, location, 0, updateHelper, threadPool::absoluteTimeInMillis,
                new NoopMappingUpdatePerformer());

        // Translog changes, because the document was deleted
        assertThat(newLocation, not(location));

        replicaRequest = bulkShardRequest.items()[0];
        replicaDeleteRequest = replicaRequest.request();
        primaryResponse = replicaRequest.getPrimaryResponse();
        response = primaryResponse.getResponse();

        // Any version can be matched on replica
        assertThat(replicaDeleteRequest.version(), equalTo(Versions.MATCH_ANY));
        assertThat(replicaDeleteRequest.versionType(), equalTo(VersionType.INTERNAL));

        assertThat(primaryResponse.getItemId(), equalTo(0));
        assertThat(primaryResponse.getId(), equalTo("id"));
        assertThat(primaryResponse.getOpType(), equalTo(DocWriteRequest.OpType.DELETE));
        assertFalse(primaryResponse.isFailed());

        assertThat(response.getResult(), equalTo(DocWriteResponse.Result.DELETED));
        assertThat(response.getShardId(), equalTo(shard.shardId()));
        assertThat(response.getIndex(), equalTo("index"));
        assertThat(response.getType(), equalTo("type"));
        assertThat(response.getId(), equalTo("id"));
        assertThat(response.getVersion(), equalTo(3L));
        assertThat(response.getSeqNo(), equalTo(2L));
        assertThat(response.forcedRefresh(), equalTo(false));

        assertDocCount(shard, 0);
        closeShards(shard);
    }

    public void testNoopUpdateReplicaRequest() throws Exception {
        DocWriteRequest writeRequest = new IndexRequest("index", "type", "id")
                .source(Requests.INDEX_CONTENT_TYPE, "field", "value");
        BulkItemRequest replicaRequest = new BulkItemRequest(0, writeRequest);

        DocWriteResponse noopUpdateResponse = new UpdateResponse(shardId, "index", "id", 0,
                DocWriteResponse.Result.NOOP);
        BulkItemResultHolder noopResults = new BulkItemResultHolder(noopUpdateResponse, null,
                replicaRequest);

        Translog.Location location = new Translog.Location(0, 0, 0);
        BulkItemRequest[] items = new BulkItemRequest[0];
        BulkShardRequest bulkShardRequest =
                new BulkShardRequest(shardId, RefreshPolicy.NONE, items);

        BulkItemResponse primaryResponse = TransportShardBulkAction.createPrimaryResponse(
                noopResults, DocWriteRequest.OpType.UPDATE, bulkShardRequest);

        Translog.Location newLocation =
                TransportShardBulkAction.calculateTranslogLocation(location, noopResults);

        // Basically nothing changes in the request since it's a noop
        assertThat(newLocation, equalTo(location));
        assertThat(primaryResponse.getItemId(), equalTo(0));
        assertThat(primaryResponse.getId(), equalTo("id"));
        assertThat(primaryResponse.getOpType(), equalTo(DocWriteRequest.OpType.UPDATE));
        assertThat(primaryResponse.getResponse(), equalTo(noopUpdateResponse));
        assertThat(primaryResponse.getResponse().getResult(),
                equalTo(DocWriteResponse.Result.NOOP));
    }

    public void testUpdateReplicaRequestWithFailure() throws Exception {
        DocWriteRequest writeRequest = new IndexRequest("index", "type", "id")
                .source(Requests.INDEX_CONTENT_TYPE, "field", "value");
        BulkItemRequest replicaRequest = new BulkItemRequest(0, writeRequest);

        Exception err = new ElasticsearchException("I'm dead <(x.x)>");
        Engine.IndexResult indexResult = new Engine.IndexResult(err, 0, 0);
        BulkItemResultHolder failedResults = new BulkItemResultHolder(null, indexResult,
                replicaRequest);

        Translog.Location location = new Translog.Location(0, 0, 0);
        BulkItemRequest[] items = new BulkItemRequest[0];
        BulkShardRequest bulkShardRequest =
                new BulkShardRequest(shardId, RefreshPolicy.NONE, items);
        BulkItemResponse primaryResponse =
                TransportShardBulkAction.createPrimaryResponse(
                        failedResults, DocWriteRequest.OpType.UPDATE, bulkShardRequest);

        Translog.Location newLocation =
                TransportShardBulkAction.calculateTranslogLocation(location, failedResults);

        // Since this was not a conflict failure, the primary response
        // should be filled out with the failure information
        assertThat(newLocation, equalTo(location));
        assertThat(primaryResponse.getItemId(), equalTo(0));
        assertThat(primaryResponse.getId(), equalTo("id"));
        assertThat(primaryResponse.getOpType(), equalTo(DocWriteRequest.OpType.INDEX));
        assertTrue(primaryResponse.isFailed());
        assertThat(primaryResponse.getFailureMessage(), containsString("I'm dead <(x.x)>"));
        BulkItemResponse.Failure failure = primaryResponse.getFailure();
        assertThat(failure.getIndex(), equalTo("index"));
        assertThat(failure.getType(), equalTo("type"));
        assertThat(failure.getId(), equalTo("id"));
        assertThat(failure.getCause(), equalTo(err));
        assertThat(failure.getStatus(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));
    }

    public void testUpdateReplicaRequestWithConflictFailure() throws Exception {
        DocWriteRequest writeRequest = new IndexRequest("index", "type", "id")
                .source(Requests.INDEX_CONTENT_TYPE, "field", "value");
        BulkItemRequest replicaRequest = new BulkItemRequest(0, writeRequest);

        Exception err = new VersionConflictEngineException(shardId, "type", "id",
                "I'm conflicted <(;_;)>");
        Engine.IndexResult indexResult = new Engine.IndexResult(err, 0, 0);
        BulkItemResultHolder failedResults = new BulkItemResultHolder(null, indexResult,
                replicaRequest);

        Translog.Location location = new Translog.Location(0, 0, 0);
        BulkItemRequest[] items = new BulkItemRequest[0];
        BulkShardRequest bulkShardRequest =
                new BulkShardRequest(shardId, RefreshPolicy.NONE, items);
        BulkItemResponse primaryResponse =
                TransportShardBulkAction.createPrimaryResponse(
                        failedResults, DocWriteRequest.OpType.UPDATE, bulkShardRequest);

        Translog.Location newLocation =
                TransportShardBulkAction.calculateTranslogLocation(location, failedResults);

        // Since this was not a conflict failure, the primary response
        // should be filled out with the failure information
        assertThat(newLocation, equalTo(location));
        assertThat(primaryResponse.getItemId(), equalTo(0));
        assertThat(primaryResponse.getId(), equalTo("id"));
        assertThat(primaryResponse.getOpType(), equalTo(DocWriteRequest.OpType.INDEX));
        assertTrue(primaryResponse.isFailed());
        assertThat(primaryResponse.getFailureMessage(), containsString("I'm conflicted <(;_;)>"));
        BulkItemResponse.Failure failure = primaryResponse.getFailure();
        assertThat(failure.getIndex(), equalTo("index"));
        assertThat(failure.getType(), equalTo("type"));
        assertThat(failure.getId(), equalTo("id"));
        assertThat(failure.getCause(), equalTo(err));
        assertThat(failure.getStatus(), equalTo(RestStatus.CONFLICT));
    }

    public void testUpdateReplicaRequestWithSuccess() throws Exception {
        DocWriteRequest writeRequest = new IndexRequest("index", "type", "id")
                .source(Requests.INDEX_CONTENT_TYPE, "field", "value");
        BulkItemRequest replicaRequest = new BulkItemRequest(0, writeRequest);

        boolean created = randomBoolean();
        Translog.Location resultLocation = new Translog.Location(42, 42, 42);
        Engine.IndexResult indexResult = new FakeResult(1, 1, created, resultLocation);
        DocWriteResponse indexResponse = new IndexResponse(shardId, "index", "id", 1, 1, created);
        BulkItemResultHolder goodResults =
                new BulkItemResultHolder(indexResponse, indexResult, replicaRequest);

        Translog.Location originalLocation = new Translog.Location(21, 21, 21);
        BulkItemRequest[] items = new BulkItemRequest[0];
        BulkShardRequest bulkShardRequest =
                new BulkShardRequest(shardId, RefreshPolicy.NONE, items);
        BulkItemResponse primaryResponse =
                TransportShardBulkAction.createPrimaryResponse(
                        goodResults, DocWriteRequest.OpType.INDEX, bulkShardRequest);

        Translog.Location newLocation =
                TransportShardBulkAction.calculateTranslogLocation(originalLocation, goodResults);

        // Check that the translog is successfully advanced
        assertThat(newLocation, equalTo(resultLocation));
        // Since this was not a conflict failure, the primary response
        // should be filled out with the failure information
        assertThat(primaryResponse.getItemId(), equalTo(0));
        assertThat(primaryResponse.getId(), equalTo("id"));
        assertThat(primaryResponse.getOpType(), equalTo(DocWriteRequest.OpType.INDEX));
        DocWriteResponse response = primaryResponse.getResponse();
        assertThat(response.status(), equalTo(created ? RestStatus.CREATED : RestStatus.OK));
    }

    public void testCalculateTranslogLocation() throws Exception {
        final Translog.Location original = new Translog.Location(0, 0, 0);

        DocWriteRequest writeRequest = new IndexRequest("index", "type", "id")
                .source(Requests.INDEX_CONTENT_TYPE, "field", "value");
        BulkItemRequest replicaRequest = new BulkItemRequest(0, writeRequest);
        BulkItemResultHolder results = new BulkItemResultHolder(null, null, replicaRequest);

        assertThat(TransportShardBulkAction.calculateTranslogLocation(original, results),
                equalTo(original));

        boolean created = randomBoolean();
        DocWriteResponse indexResponse = new IndexResponse(shardId, "index", "id", 1, 1, created);
        Translog.Location newLocation = new Translog.Location(1, 1, 1);
        Engine.IndexResult indexResult = new IndexResultWithLocation(randomNonNegativeLong(),
                randomNonNegativeLong(), created, newLocation);
        results = new BulkItemResultHolder(indexResponse, indexResult, replicaRequest);
        assertThat(TransportShardBulkAction.calculateTranslogLocation(original, results),
                equalTo(newLocation));

    }

    public class IndexResultWithLocation extends Engine.IndexResult {
        private final Translog.Location location;
        public IndexResultWithLocation(long version, long seqNo, boolean created,
                                       Translog.Location newLocation) {
            super(version, seqNo, created);
            this.location = newLocation;
        }

        @Override
        public Translog.Location getTranslogLocation() {
            return this.location;
        }
    }

    public void testPrepareIndexOpOnReplica() throws Exception {
        IndexMetaData metaData = indexMetaData();
        IndexShard shard = newStartedShard(false);

        DocWriteResponse primaryResponse = new IndexResponse(shardId, "index", "id",
                1, 1, randomBoolean());
        IndexRequest request = new IndexRequest("index", "type", "id")
                .source(Requests.INDEX_CONTENT_TYPE, "field", "value");

        Engine.Index op = TransportShardBulkAction.prepareIndexOperationOnReplica(
                primaryResponse, request, shard);

        assertThat(op.version(), equalTo(primaryResponse.getVersion()));
        assertThat(op.seqNo(), equalTo(primaryResponse.getSeqNo()));
        assertThat(op.versionType(), equalTo(VersionType.EXTERNAL));

        closeShards(shard);
    }

    /**
     * Fake IndexResult that has a settable translog location
     */
    private static class FakeResult extends Engine.IndexResult {

        private final Translog.Location location;

        protected FakeResult(long version, long seqNo, boolean created,
                             Translog.Location location) {
            super(version, seqNo, created);
            this.location = location;
        }

        @Override
        public Translog.Location getTranslogLocation() {
            return this.location;
        }
    }

    /** Doesn't perform any mapping updates */
    public static class NoopMappingUpdatePerformer implements MappingUpdatePerformer {
        public void updateMappingsIfNeeded(Engine.Index operation,
                                           ShardId shardId,
                                           String type) throws Exception {
        }

        public void verifyMappings(Engine.Index operation, ShardId shardId) throws Exception {
        }
    }

    /** Always throw the given exception */
    private class ThrowingMappingUpdatePerformer implements MappingUpdatePerformer {
        private final Exception e;
        ThrowingMappingUpdatePerformer(Exception e) {
            this.e = e;
        }

        public void updateMappingsIfNeeded(Engine.Index operation,
                                           ShardId shardId,
                                           String type) throws Exception {
            throw e;
        }

        public void verifyMappings(Engine.Index operation, ShardId shardId) throws Exception {
            fail("should not have gotten to this point");
        }
    }

    /** Always throw the given exception */
    private class ThrowingVerifyingMappingUpdatePerformer implements MappingUpdatePerformer {
        private final Exception e;
        ThrowingVerifyingMappingUpdatePerformer(Exception e) {
            this.e = e;
        }

        public void updateMappingsIfNeeded(Engine.Index operation,
                                           ShardId shardId,
                                           String type) throws Exception {
        }

        public void verifyMappings(Engine.Index operation, ShardId shardId) throws Exception {
            throw e;
        }
    }
}
