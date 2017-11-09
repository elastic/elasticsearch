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
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.Version;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.TransportShardBulkAction.ReplicaItemExecutionMode;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.support.replication.ReplicationOperation;
import org.elasticsearch.action.support.replication.TransportWriteAction.WritePrimaryResult;
import org.elasticsearch.action.update.UpdateHelper;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

import static org.elasticsearch.action.bulk.TransportShardBulkAction.replicaItemExecutionMode;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TransportShardBulkActionTests extends IndexShardTestCase {

    private final ShardId shardId = new ShardId("index", "_na_", 0);
    private final Settings idxSettings = Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put("index.version.created", Version.CURRENT.id)
            .build();

    private IndexMetaData indexMetaData() throws IOException {
        return IndexMetaData.builder("index")
                .putMapping("type",
                        "{\"properties\":{\"foo\":{\"type\":\"text\",\"fields\":" +
                                "{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}}}}")
                .settings(idxSettings)
                .primaryTerm(0, 1).build();
    }

    public void testShouldExecuteReplicaItem() throws Exception {
        // Successful index request should be replicated
        DocWriteRequest writeRequest = new IndexRequest("index", "type", "id")
                .source(Requests.INDEX_CONTENT_TYPE, "foo", "bar");
        DocWriteResponse response = new IndexResponse(shardId, "type", "id", 1, 17, 1, randomBoolean());
        BulkItemRequest request = new BulkItemRequest(0, writeRequest);
        request.setPrimaryResponse(new BulkItemResponse(0, DocWriteRequest.OpType.INDEX, response));
        assertThat(replicaItemExecutionMode(request, 0),
                equalTo(ReplicaItemExecutionMode.NORMAL));

        // Failed index requests without sequence no should not be replicated
        writeRequest = new IndexRequest("index", "type", "id")
                .source(Requests.INDEX_CONTENT_TYPE, "foo", "bar");
        request = new BulkItemRequest(0, writeRequest);
        request.setPrimaryResponse(
                new BulkItemResponse(0, DocWriteRequest.OpType.INDEX,
                        new BulkItemResponse.Failure("index", "type", "id",
                                                     new IllegalArgumentException("i died"))));
        assertThat(replicaItemExecutionMode(request, 0),
                equalTo(ReplicaItemExecutionMode.NOOP));

        // Failed index requests with sequence no should be replicated
        request = new BulkItemRequest(0, writeRequest);
        request.setPrimaryResponse(
                new BulkItemResponse(0, DocWriteRequest.OpType.INDEX,
                        new BulkItemResponse.Failure("index", "type", "id",
                                new IllegalArgumentException(
                                        "i died after sequence no was generated"),
                                1)));
        assertThat(replicaItemExecutionMode(request, 0),
                equalTo(ReplicaItemExecutionMode.FAILURE));
        // NOOP requests should not be replicated
        writeRequest = new UpdateRequest("index", "type", "id");
        response = new UpdateResponse(shardId, "type", "id", 1, DocWriteResponse.Result.NOOP);
        request = new BulkItemRequest(0, writeRequest);
        request.setPrimaryResponse(new BulkItemResponse(0, DocWriteRequest.OpType.UPDATE,
                        response));
        assertThat(replicaItemExecutionMode(request, 0),
                equalTo(ReplicaItemExecutionMode.NOOP));
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

    public void testSkipBulkIndexRequestIfAborted() throws Exception {
        IndexShard shard = newStartedShard(true);

        BulkItemRequest[] items = new BulkItemRequest[randomIntBetween(2, 5)];
        for (int i = 0; i < items.length; i++) {
            DocWriteRequest writeRequest = new IndexRequest("index", "type", "id_" + i)
                .source(Requests.INDEX_CONTENT_TYPE, "foo", "bar-" + i)
                .opType(DocWriteRequest.OpType.INDEX);
            items[i] = new BulkItemRequest(i, writeRequest);
        }
        BulkShardRequest bulkShardRequest = new BulkShardRequest(shardId, RefreshPolicy.NONE, items);

        // Preemptively abort one of the bulk items, but allow the others to proceed
        BulkItemRequest rejectItem = randomFrom(items);
        RestStatus rejectionStatus = randomFrom(RestStatus.BAD_REQUEST, RestStatus.CONFLICT, RestStatus.FORBIDDEN, RestStatus.LOCKED);
        final ElasticsearchStatusException rejectionCause = new ElasticsearchStatusException("testing rejection", rejectionStatus);
        rejectItem.abort("index", rejectionCause);

        UpdateHelper updateHelper = null;
        WritePrimaryResult<BulkShardRequest, BulkShardResponse> result = TransportShardBulkAction.performOnPrimary(
            bulkShardRequest, shard, updateHelper, threadPool::absoluteTimeInMillis, new NoopMappingUpdatePerformer());

        // since at least 1 item passed, the tran log location should exist,
        assertThat(result.location, notNullValue());
        // and the response should exist and match the item count
        assertThat(result.finalResponseIfSuccessful, notNullValue());
        assertThat(result.finalResponseIfSuccessful.getResponses(), arrayWithSize(items.length));

        // check each response matches the input item, including the rejection
        for (int i = 0; i < items.length; i++) {
            BulkItemResponse response = result.finalResponseIfSuccessful.getResponses()[i];
            assertThat(response.getItemId(), equalTo(i));
            assertThat(response.getIndex(), equalTo("index"));
            assertThat(response.getType(), equalTo("type"));
            assertThat(response.getId(), equalTo("id_" + i));
            assertThat(response.getOpType(), equalTo(DocWriteRequest.OpType.INDEX));
            if (response.getItemId() == rejectItem.id()) {
                assertTrue(response.isFailed());
                assertThat(response.getFailure().getCause(), equalTo(rejectionCause));
                assertThat(response.status(), equalTo(rejectionStatus));
            } else {
                assertFalse(response.isFailed());
            }
        }

        // Check that the non-rejected updates made it to the shard
        assertDocCount(shard, items.length - 1);
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

        // Pretend the mappings haven't made it to the node yet, and throw a rejection
        RuntimeException err = new ReplicationOperation.RetryOnPrimaryException(shardId, "rejection");

        try {
            TransportShardBulkAction.executeBulkItemRequest(metaData, shard, bulkShardRequest,
                    location, 0, updateHelper, threadPool::absoluteTimeInMillis,
                    new ThrowingVerifyingMappingUpdatePerformer(err));
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
        RuntimeException err = new IllegalArgumentException("mapping conflict");

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
        DocWriteResponse indexResponse = new IndexResponse(shardId, "index", "id", 1, 17, 1, created);
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
        DocWriteResponse indexResponse = new IndexResponse(shardId, "index", "id", 1, 17, 1, created);
        Translog.Location newLocation = new Translog.Location(1, 1, 1);
        final long version = randomNonNegativeLong();
        final long seqNo = randomNonNegativeLong();
        Engine.IndexResult indexResult = new IndexResultWithLocation(version, seqNo, created, newLocation);
        results = new BulkItemResultHolder(indexResponse, indexResult, replicaRequest);
        assertThat(TransportShardBulkAction.calculateTranslogLocation(original, results),
                equalTo(newLocation));

    }

    public void testNoOpReplicationOnPrimaryDocumentFailure() throws Exception {
        final IndexShard shard = spy(newStartedShard(false));
        BulkItemRequest itemRequest = new BulkItemRequest(0,
                new IndexRequest("index", "type")
                        .source(Requests.INDEX_CONTENT_TYPE, "foo", "bar")
        );
        final String failureMessage = "simulated primary failure";
        final IOException exception = new IOException(failureMessage);
        itemRequest.setPrimaryResponse(new BulkItemResponse(0,
                randomFrom(
                        DocWriteRequest.OpType.CREATE,
                        DocWriteRequest.OpType.DELETE,
                        DocWriteRequest.OpType.INDEX
                ),
                new BulkItemResponse.Failure("index", "type", "1",
                    exception, 1L)
        ));
        BulkItemRequest[] itemRequests = new BulkItemRequest[1];
        itemRequests[0] = itemRequest;
        BulkShardRequest bulkShardRequest = new BulkShardRequest(
                shard.shardId(), RefreshPolicy.NONE, itemRequests);
        TransportShardBulkAction.performOnReplica(bulkShardRequest, shard);
        verify(shard, times(1)).markSeqNoAsNoop(1, exception.toString());
        closeShards(shard);
    }

    public void testMappingUpdateParsesCorrectNumberOfTimes() throws Exception {
        IndexMetaData metaData = indexMetaData();
        logger.info("--> metadata.getIndex(): {}", metaData.getIndex());
        final IndexShard shard = spy(newStartedShard(true));

        IndexRequest request = new IndexRequest("index", "type", "id")
                .source(Requests.INDEX_CONTENT_TYPE, "foo", "bar");

        final AtomicInteger updateCalled = new AtomicInteger(0);
        final AtomicInteger verifyCalled = new AtomicInteger(0);
        TransportShardBulkAction.executeIndexRequestOnPrimary(request, shard,
                new MappingUpdatePerformer() {
                    @Override
                    public void updateMappings(Mapping update, ShardId shardId, String type) {
                        // There should indeed be a mapping update
                        assertNotNull(update);
                        updateCalled.incrementAndGet();
                    }

                    @Override
                    public void verifyMappings(Mapping update, ShardId shardId) {
                        // No-op, will be called
                        logger.info("--> verifying mappings noop");
                        verifyCalled.incrementAndGet();
                    }
        });

        assertThat("mappings were \"updated\" once", updateCalled.get(), equalTo(1));
        assertThat("mappings were \"verified\" once", verifyCalled.get(), equalTo(1));

        // Verify that the shard "executed" the operation twice
        verify(shard, times(2)).applyIndexOperationOnPrimary(anyLong(), any(), any(), anyLong(), anyBoolean(), any());

        // Update the mapping, so the next mapping updater doesn't do anything
        final MapperService mapperService = shard.mapperService();
        logger.info("--> mapperService.index(): {}", mapperService.index());
        mapperService.updateMapping(metaData);

        TransportShardBulkAction.executeIndexRequestOnPrimary(request, shard,
                new MappingUpdatePerformer() {
                    @Override
                    public void updateMappings(Mapping update, ShardId shardId, String type) {
                        fail("should not have had to update the mappings");
                    }

                    @Override
                    public void verifyMappings(Mapping update, ShardId shardId) {
                        fail("should not have had to update the mappings");
                    }
        });

        // Verify that the shard "executed" the operation only once (2 for previous invocations plus
        // 1 for this execution)
        verify(shard, times(3)).applyIndexOperationOnPrimary(anyLong(), any(), any(), anyLong(), anyBoolean(), any());

        closeShards(shard);
    }

    public class IndexResultWithLocation extends Engine.IndexResult {
        private final Translog.Location location;
        public IndexResultWithLocation(long version, long seqNo, boolean created, Translog.Location newLocation) {
            super(version, seqNo, created);
            this.location = newLocation;
        }

        @Override
        public Translog.Location getTranslogLocation() {
            return this.location;
        }
    }

    public void testProcessUpdateResponse() throws Exception {
        IndexShard shard = newStartedShard(false);

        UpdateRequest updateRequest = new UpdateRequest("index", "type", "id");
        BulkItemRequest request = new BulkItemRequest(0, updateRequest);
        Exception err = new VersionConflictEngineException(shardId, "type", "id",
                "I'm conflicted <(;_;)>");
        Engine.IndexResult indexResult = new Engine.IndexResult(err, 0, 0);
        Engine.DeleteResult deleteResult = new Engine.DeleteResult(1, 1, true);
        DocWriteResponse.Result docWriteResult = DocWriteResponse.Result.CREATED;
        DocWriteResponse.Result deleteWriteResult = DocWriteResponse.Result.DELETED;
        IndexRequest indexRequest = new IndexRequest("index", "type", "id");
        DeleteRequest deleteRequest = new DeleteRequest("index", "type", "id");
        UpdateHelper.Result translate = new UpdateHelper.Result(indexRequest, docWriteResult,
                new HashMap<String, Object>(), XContentType.JSON);
        UpdateHelper.Result translateDelete = new UpdateHelper.Result(deleteRequest, deleteWriteResult,
                new HashMap<String, Object>(), XContentType.JSON);

        BulkItemRequest[] itemRequests = new BulkItemRequest[1];
        itemRequests[0] = request;
        BulkShardRequest bulkShardRequest = new BulkShardRequest(shard.shardId(), RefreshPolicy.NONE, itemRequests);

        BulkItemResultHolder holder = TransportShardBulkAction.processUpdateResponse(updateRequest,
                "index", indexResult, translate, shard, 7);

        assertTrue(holder.isVersionConflict());
        assertThat(holder.response, instanceOf(UpdateResponse.class));
        UpdateResponse updateResp = (UpdateResponse) holder.response;
        assertThat(updateResp.getGetResult(), equalTo(null));
        assertThat(holder.operationResult, equalTo(indexResult));
        BulkItemRequest replicaBulkRequest = holder.replicaRequest;
        assertThat(replicaBulkRequest.id(), equalTo(7));
        DocWriteRequest replicaRequest = replicaBulkRequest.request();
        assertThat(replicaRequest, instanceOf(IndexRequest.class));
        assertThat(replicaRequest, equalTo(indexRequest));

        BulkItemResultHolder deleteHolder = TransportShardBulkAction.processUpdateResponse(updateRequest,
                "index", deleteResult, translateDelete, shard, 8);

        assertFalse(deleteHolder.isVersionConflict());
        assertThat(deleteHolder.response, instanceOf(UpdateResponse.class));
        UpdateResponse delUpdateResp = (UpdateResponse) deleteHolder.response;
        assertThat(delUpdateResp.getGetResult(), equalTo(null));
        assertThat(deleteHolder.operationResult, equalTo(deleteResult));
        BulkItemRequest delReplicaBulkRequest = deleteHolder.replicaRequest;
        assertThat(delReplicaBulkRequest.id(), equalTo(8));
        DocWriteRequest delReplicaRequest = delReplicaBulkRequest.request();
        assertThat(delReplicaRequest, instanceOf(DeleteRequest.class));
        assertThat(delReplicaRequest, equalTo(deleteRequest));

        closeShards(shard);
    }

    public void testExecuteUpdateRequestOnce() throws Exception {
        IndexMetaData metaData = indexMetaData();
        IndexShard shard = newStartedShard(true);

        Map<String, Object> source = new HashMap<>();
        source.put("foo", "bar");
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
        IndexRequest indexRequest = new IndexRequest("index", "type", "id");
        indexRequest.source(source);

        DocWriteResponse.Result docWriteResult = DocWriteResponse.Result.CREATED;
        UpdateHelper.Result translate = new UpdateHelper.Result(indexRequest, docWriteResult,
                new HashMap<String, Object>(), XContentType.JSON);
        UpdateHelper updateHelper = new MockUpdateHelper(translate);
        UpdateRequest updateRequest = new UpdateRequest("index", "type", "id");
        updateRequest.upsert(source);

        BulkItemResultHolder holder = TransportShardBulkAction.executeUpdateRequestOnce(updateRequest, shard, metaData,
                "index", updateHelper, threadPool::absoluteTimeInMillis, primaryRequest, 0, new NoopMappingUpdatePerformer());

        assertFalse(holder.isVersionConflict());
        assertNotNull(holder.response);
        assertNotNull(holder.operationResult);
        assertNotNull(holder.replicaRequest);

        assertThat(holder.response, instanceOf(UpdateResponse.class));
        UpdateResponse updateResp = (UpdateResponse) holder.response;
        assertThat(updateResp.getGetResult(), equalTo(null));
        BulkItemRequest replicaBulkRequest = holder.replicaRequest;
        assertThat(replicaBulkRequest.id(), equalTo(0));
        DocWriteRequest replicaRequest = replicaBulkRequest.request();
        assertThat(replicaRequest, instanceOf(IndexRequest.class));
        assertThat(replicaRequest, equalTo(indexRequest));

        // Assert that the document actually made it there
        assertDocCount(shard, 1);
        closeShards(shard);
    }

    public void testExecuteUpdateRequestOnceWithFailure() throws Exception {
        IndexMetaData metaData = indexMetaData();
        IndexShard shard = newStartedShard(true);

        Map<String, Object> source = new HashMap<>();
        source.put("foo", "bar");
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
        IndexRequest indexRequest = new IndexRequest("index", "type", "id");
        indexRequest.source(source);

        DocWriteResponse.Result docWriteResult = DocWriteResponse.Result.CREATED;
        Exception prepareFailure = new IllegalArgumentException("I failed to do something!");
        UpdateHelper updateHelper = new FailingUpdateHelper(prepareFailure);
        UpdateRequest updateRequest = new UpdateRequest("index", "type", "id");
        updateRequest.upsert(source);

        BulkItemResultHolder holder = TransportShardBulkAction.executeUpdateRequestOnce(updateRequest, shard, metaData,
                "index", updateHelper, threadPool::absoluteTimeInMillis, primaryRequest, 0, new NoopMappingUpdatePerformer());

        assertFalse(holder.isVersionConflict());
        assertNull(holder.response);
        assertNotNull(holder.operationResult);
        assertNotNull(holder.replicaRequest);

        Engine.IndexResult opResult = (Engine.IndexResult) holder.operationResult;
        assertTrue(opResult.hasFailure());
        assertFalse(opResult.isCreated());
        Exception e = opResult.getFailure();
        assertThat(e.getMessage(), containsString("I failed to do something!"));

        BulkItemRequest replicaBulkRequest = holder.replicaRequest;
        assertThat(replicaBulkRequest.id(), equalTo(0));
        assertThat(replicaBulkRequest.request(), instanceOf(IndexRequest.class));
        IndexRequest replicaRequest = (IndexRequest) replicaBulkRequest.request();
        assertThat(replicaRequest.index(), equalTo("index"));
        assertThat(replicaRequest.type(), equalTo("type"));
        assertThat(replicaRequest.id(), equalTo("id"));
        assertThat(replicaRequest.sourceAsMap(), equalTo(source));

        // Assert that the document did not make it there, since it should have failed
        assertDocCount(shard, 0);
        closeShards(shard);
    }

    /**
     * Fake UpdateHelper that always returns whatever result you give it
     */
    private static class MockUpdateHelper extends UpdateHelper {
        private final UpdateHelper.Result result;

        MockUpdateHelper(UpdateHelper.Result result) {
            super(Settings.EMPTY, null);
            this.result = result;
        }

        @Override
        public UpdateHelper.Result prepare(UpdateRequest u, IndexShard s, LongSupplier n) {
            logger.info("--> preparing update for {} - {}", s, u);
            return result;
        }
    }

    /**
     * An update helper that always fails to prepare the update
     */
    private static class FailingUpdateHelper extends UpdateHelper {
        private final Exception e;

        FailingUpdateHelper(Exception failure) {
            super(Settings.EMPTY, null);
            this.e = failure;
        }

        @Override
        public UpdateHelper.Result prepare(UpdateRequest u, IndexShard s, LongSupplier n) {
            logger.info("--> preparing failing update for {} - {}", s, u);
            throw new ElasticsearchException(e);
        }
    }

    /**
     * Fake IndexResult that has a settable translog location
     */
    private static class FakeResult extends Engine.IndexResult {

        private final Translog.Location location;

        protected FakeResult(long version, long seqNo, boolean created, Translog.Location location) {
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
        public void updateMappings(Mapping update, ShardId shardId, String type) {
        }

        public void verifyMappings(Mapping update, ShardId shardId) {
        }
    }

    /** Always throw the given exception */
    private class ThrowingMappingUpdatePerformer implements MappingUpdatePerformer {
        private final RuntimeException e;
        ThrowingMappingUpdatePerformer(RuntimeException e) {
            this.e = e;
        }

        public void updateMappings(Mapping update, ShardId shardId, String type) {
            throw e;
        }

        public void verifyMappings(Mapping update, ShardId shardId) {
            fail("should not have gotten to this point");
        }
    }

    /** Always throw the given exception */
    private class ThrowingVerifyingMappingUpdatePerformer implements MappingUpdatePerformer {
        private final RuntimeException e;
        ThrowingVerifyingMappingUpdatePerformer(RuntimeException e) {
            this.e = e;
        }

        public void updateMappings(Mapping update, ShardId shardId, String type) {
        }

        public void verifyMappings(Mapping update, ShardId shardId) {
            throw e;
        }
    }
}
