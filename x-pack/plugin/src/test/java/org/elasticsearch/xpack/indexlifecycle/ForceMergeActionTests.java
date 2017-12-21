/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.elasticsearch.action.admin.indices.segments.IndexSegments;
import org.elasticsearch.action.admin.indices.segments.IndexShardSegments;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.elasticsearch.action.admin.indices.segments.ShardSegments;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.engine.Segment;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.indexlifecycle.LifecycleAction.Listener;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;

public class ForceMergeActionTests extends AbstractSerializingTestCase<ForceMergeAction> {

    @Override
    protected ForceMergeAction doParseInstance(XContentParser parser) throws IOException {
        return ForceMergeAction.parse(parser);
    }

    @Override
    protected ForceMergeAction createTestInstance() {
        return new ForceMergeAction(randomIntBetween(1, 100));
    }

    @Override
    protected ForceMergeAction mutateInstance(ForceMergeAction instance) {
        return new ForceMergeAction(instance.getMaxNumSegments() + randomIntBetween(1, 10));
    }

    @Override
    protected Reader<ForceMergeAction> instanceReader() {
        return ForceMergeAction::new;
    }

    public void testMissingMaxNumSegments() throws IOException {
        BytesReference emptyObject = JsonXContent.contentBuilder().startObject().endObject().bytes();
        XContentParser parser = XContentHelper.createParser(null, emptyObject, XContentType.JSON);
        Exception e = expectThrows(IllegalArgumentException.class, () -> ForceMergeAction.parse(parser));
        assertThat(e.getMessage(), equalTo("Required [max_num_segments]"));
    }

    public void testInvalidNegativeSegmentNumber() {
        Exception r = expectThrows(IllegalArgumentException.class, () -> new ForceMergeAction(randomIntBetween(-10, 0)));
        assertThat(r.getMessage(), equalTo("[max_num_segments] must be a positive integer"));
    }

    public void testExecuteSuccessfully() throws Exception {
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));

        ClusterService clusterService = Mockito.mock(ClusterService.class);
        IndexMetaData indexMetadata = IndexMetaData.builder(index.getName())
            .settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        ImmutableOpenMap.Builder<String, IndexMetaData> indices = ImmutableOpenMap.<String, IndexMetaData> builder().fPut(index.getName(),
            indexMetadata);
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).metaData(MetaData.builder().indices(indices.build()))
            .build();
        Mockito.when(clusterService.state()).thenReturn(clusterState);
        Client client = Mockito.mock(Client.class);
        AdminClient adminClient = Mockito.mock(AdminClient.class);
        IndicesAdminClient indicesClient = Mockito.mock(IndicesAdminClient.class);
        UpdateSettingsResponse updateSettingsResponse = Mockito.mock(UpdateSettingsResponse.class);
        IndicesSegmentResponse indicesSegmentResponse = Mockito.mock(IndicesSegmentResponse.class);
        IndexSegments indexSegments = Mockito.mock(IndexSegments.class);
        IndexShardSegments indexShardSegments = Mockito.mock(IndexShardSegments.class);
        Map<Integer, IndexShardSegments> indexShards = Collections.singletonMap(0, indexShardSegments);
        ShardSegments shardSegmentsOne = Mockito.mock(ShardSegments.class);
        ShardSegments[] shardSegmentsArray = new ShardSegments[] { shardSegmentsOne };
        Spliterator<IndexShardSegments> iss = indexShards.values().spliterator();
        List<Segment> segments = Arrays.asList(null, null);
        ForceMergeResponse forceMergeResponse = Mockito.mock(ForceMergeResponse.class);
        Mockito.when(forceMergeResponse.getStatus()).thenReturn(RestStatus.OK);
        Mockito.when(indicesSegmentResponse.getStatus()).thenReturn(RestStatus.OK);
        Mockito.when(indicesSegmentResponse.getIndices()).thenReturn(Collections.singletonMap(index.getName(), indexSegments));
        Mockito.when(indexSegments.spliterator()).thenReturn(iss);
        Mockito.when(indexShardSegments.getShards()).thenReturn(shardSegmentsArray);
        Mockito.when(shardSegmentsOne.getSegments()).thenReturn(segments);
        int maxNumSegments = 1;

        Mockito.when(client.admin()).thenReturn(adminClient);
        Mockito.when(adminClient.indices()).thenReturn(indicesClient);

        Mockito.doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<IndicesSegmentResponse> listener = (ActionListener<IndicesSegmentResponse>) invocationOnMock.getArguments()[1];
            listener.onResponse(indicesSegmentResponse);
            return null;
        }).when(indicesClient).segments(any(), any());

        Mockito.doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<UpdateSettingsResponse> listener = (ActionListener<UpdateSettingsResponse>) invocationOnMock.getArguments()[1];
            listener.onResponse(updateSettingsResponse);
            return null;
        }).when(indicesClient).updateSettings(any(), any());

        Mockito.doAnswer(invocationOnMock -> {
            ForceMergeRequest request = (ForceMergeRequest) invocationOnMock.getArguments()[0];
            assertThat(request.maxNumSegments(), equalTo(maxNumSegments));
            @SuppressWarnings("unchecked")
            ActionListener<ForceMergeResponse> listener = (ActionListener<ForceMergeResponse>) invocationOnMock.getArguments()[1];
            listener.onResponse(forceMergeResponse);
            return null;
        }).when(indicesClient).forceMerge(any(), any());

        SetOnce<Boolean> actionCompleted = new SetOnce<>();
        ForceMergeAction action = new ForceMergeAction(maxNumSegments);
        action.execute(index, client, clusterService, new Listener() {

            @Override
            public void onSuccess(boolean completed) {
                actionCompleted.set(completed);
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("Unexpected method call", e);
            }
        });

        assertEquals(true, actionCompleted.get());

        Mockito.verify(client, Mockito.atLeast(1)).admin();
        Mockito.verify(adminClient, Mockito.atLeast(1)).indices();
        Mockito.verify(indicesClient, Mockito.atLeast(1)).segments(any(), any());
        Mockito.verify(indicesClient, Mockito.atLeast(1)).updateSettings(any(), any());
        Mockito.verify(indicesClient, Mockito.atLeast(1)).forceMerge(any(), any());
        Mockito.verify(clusterService, Mockito.atLeast(1)).state();
    }

    public void testExecuteWhenReadOnlyAlready() {
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        boolean isReadOnlyAlready = true;

        ClusterService clusterService = Mockito.mock(ClusterService.class);
        IndexMetaData indexMetadata = IndexMetaData.builder(index.getName())
            .settings(settings(Version.CURRENT).put(IndexMetaData.SETTING_BLOCKS_WRITE, isReadOnlyAlready))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        ImmutableOpenMap.Builder<String, IndexMetaData> indices = ImmutableOpenMap.<String, IndexMetaData> builder().fPut(index.getName(),
            indexMetadata);
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).metaData(MetaData.builder().indices(indices.build()))
            .build();
        Mockito.when(clusterService.state()).thenReturn(clusterState);

        Client client = Mockito.mock(Client.class);
        AdminClient adminClient = Mockito.mock(AdminClient.class);
        IndicesAdminClient indicesClient = Mockito.mock(IndicesAdminClient.class);
        IndicesSegmentResponse indicesSegmentResponse = Mockito.mock(IndicesSegmentResponse.class);
        IndexSegments indexSegments = Mockito.mock(IndexSegments.class);
        IndexShardSegments indexShardSegments = Mockito.mock(IndexShardSegments.class);
        Map<Integer, IndexShardSegments> indexShards = Collections.singletonMap(0, indexShardSegments);
        ShardSegments shardSegmentsOne = Mockito.mock(ShardSegments.class);
        ShardSegments[] shardSegmentsArray = new ShardSegments[] { shardSegmentsOne };
        Spliterator<IndexShardSegments> iss = indexShards.values().spliterator();
        List<Segment> segments = Arrays.asList(null, null);

        Mockito.when(indicesSegmentResponse.getStatus()).thenReturn(RestStatus.OK);
        Mockito.when(indicesSegmentResponse.getIndices()).thenReturn(Collections.singletonMap(index.getName(), indexSegments));
        Mockito.when(indexSegments.spliterator()).thenReturn(iss);
        Mockito.when(indexShardSegments.getShards()).thenReturn(shardSegmentsArray);
        Mockito.when(shardSegmentsOne.getSegments()).thenReturn(segments);
        int maxNumSegments = Integer.MAX_VALUE;

        Mockito.when(client.admin()).thenReturn(adminClient);
        Mockito.when(adminClient.indices()).thenReturn(indicesClient);


        Mockito.doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<IndicesSegmentResponse> listener = (ActionListener<IndicesSegmentResponse>) invocationOnMock.getArguments()[1];
            listener.onResponse(indicesSegmentResponse);
            return null;
        }).when(indicesClient).segments(any(), any());


        SetOnce<Boolean> actionCompleted = new SetOnce<>();
        ForceMergeAction action = new ForceMergeAction(maxNumSegments);
        action.execute(index, client, clusterService, new Listener() {

            @Override
            public void onSuccess(boolean completed) {
                actionCompleted.set(completed);
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("Unexpected method call", e);
            }
        });

        assertEquals(null, actionCompleted.get());

        InOrder inOrder = Mockito.inOrder(clusterService, client, adminClient, indicesClient);
        inOrder.verify(clusterService).state();
        inOrder.verify(client).admin();
        inOrder.verify(adminClient).indices();
        inOrder.verify(indicesClient).segments(any(), any());
        inOrder.verify(indicesClient).updateSettings(any(), any());
        Mockito.verify(indicesClient, Mockito.never()).forceMerge(any(), any());
    }

    public void testExecuteWithNoNeedToForceMerge() {
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        boolean isReadOnlyAlready = false;

        ClusterService clusterService = Mockito.mock(ClusterService.class);
        IndexMetaData indexMetadata = IndexMetaData.builder(index.getName())
            .settings(settings(Version.CURRENT).put(IndexMetaData.SETTING_BLOCKS_WRITE, isReadOnlyAlready))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        ImmutableOpenMap.Builder<String, IndexMetaData> indices = ImmutableOpenMap.<String, IndexMetaData> builder().fPut(index.getName(),
            indexMetadata);
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).metaData(MetaData.builder().indices(indices.build()))
            .build();
        Mockito.when(clusterService.state()).thenReturn(clusterState);

        Client client = Mockito.mock(Client.class);
        AdminClient adminClient = Mockito.mock(AdminClient.class);
        IndicesAdminClient indicesClient = Mockito.mock(IndicesAdminClient.class);
        IndicesSegmentResponse indicesSegmentResponse = Mockito.mock(IndicesSegmentResponse.class);
        IndexSegments indexSegments = Mockito.mock(IndexSegments.class);
        IndexShardSegments indexShardSegments = Mockito.mock(IndexShardSegments.class);
        Map<Integer, IndexShardSegments> indexShards = Collections.singletonMap(0, indexShardSegments);
        ShardSegments shardSegmentsOne = Mockito.mock(ShardSegments.class);
        ShardSegments[] shardSegmentsArray = new ShardSegments[] { shardSegmentsOne };
        Spliterator<IndexShardSegments> iss = indexShards.values().spliterator();
        List<Segment> segments = Arrays.asList(null, null);

        Mockito.when(indicesSegmentResponse.getStatus()).thenReturn(RestStatus.OK);
        Mockito.when(indicesSegmentResponse.getIndices()).thenReturn(Collections.singletonMap(index.getName(), indexSegments));
        Mockito.when(indexSegments.spliterator()).thenReturn(iss);
        Mockito.when(indexShardSegments.getShards()).thenReturn(shardSegmentsArray);
        Mockito.when(shardSegmentsOne.getSegments()).thenReturn(segments);
        int maxNumSegments = Integer.MAX_VALUE;

        Mockito.when(client.admin()).thenReturn(adminClient);
        Mockito.when(adminClient.indices()).thenReturn(indicesClient);


        Mockito.doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<IndicesSegmentResponse> listener = (ActionListener<IndicesSegmentResponse>) invocationOnMock.getArguments()[1];
            listener.onResponse(indicesSegmentResponse);
            return null;
        }).when(indicesClient).segments(any(), any());


        SetOnce<Boolean> actionCompleted = new SetOnce<>();
        ForceMergeAction action = new ForceMergeAction(maxNumSegments);
        action.execute(index, client, clusterService, new Listener() {

            @Override
            public void onSuccess(boolean completed) {
                actionCompleted.set(completed);
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("Unexpected method call", e);
            }
        });

        assertEquals(null, actionCompleted.get());

        InOrder inOrder = Mockito.inOrder(clusterService, client, adminClient, indicesClient);
        inOrder.verify(clusterService).state();
        inOrder.verify(client).admin();
        inOrder.verify(adminClient).indices();
        inOrder.verify(indicesClient).segments(any(), any());
        Mockito.verify(indicesClient, Mockito.never()).updateSettings(any(), any());
        Mockito.verify(indicesClient, Mockito.never()).forceMerge(any(), any());
    }

    public void testCheckSegmentsNeedsMerging() {
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        Client client = Mockito.mock(Client.class);
        AdminClient adminClient = Mockito.mock(AdminClient.class);
        IndicesAdminClient indicesClient = Mockito.mock(IndicesAdminClient.class);
        IndicesSegmentResponse indicesSegmentResponse = Mockito.mock(IndicesSegmentResponse.class);
        IndexSegments indexSegments = Mockito.mock(IndexSegments.class);
        IndexShardSegments indexShardSegments = Mockito.mock(IndexShardSegments.class);
        Map<Integer, IndexShardSegments> indexShards = Collections.singletonMap(0, indexShardSegments);
        ShardSegments shardSegmentsOne = Mockito.mock(ShardSegments.class);
        ShardSegments[] shardSegmentsArray = new ShardSegments[] { shardSegmentsOne };
        Spliterator<IndexShardSegments> iss = indexShards.values().spliterator();
        List<Segment> segments = Arrays.asList(null, null);
        Mockito.when(indicesSegmentResponse.getStatus()).thenReturn(RestStatus.OK);
        Mockito.when(indicesSegmentResponse.getIndices()).thenReturn(Collections.singletonMap(index.getName(), indexSegments));
        Mockito.when(indexSegments.spliterator()).thenReturn(iss);
        Mockito.when(indexShardSegments.getShards()).thenReturn(shardSegmentsArray);
        Mockito.when(shardSegmentsOne.getSegments()).thenReturn(segments);
        int maxNumSegments = 1;

        Mockito.when(client.admin()).thenReturn(adminClient);
        Mockito.when(adminClient.indices()).thenReturn(indicesClient);

        Mockito.doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<IndicesSegmentResponse> listener = (ActionListener<IndicesSegmentResponse>) invocationOnMock.getArguments()[1];
            listener.onResponse(indicesSegmentResponse);
            return null;
        }).when(indicesClient).segments(any(), any());

        SetOnce<Boolean> nextActionCalled = new SetOnce<>();
        ForceMergeAction action = new ForceMergeAction(maxNumSegments);
        action.checkSegments(index, client, new Listener() {

            @Override
            public void onSuccess(boolean completed) {
                throw new AssertionError("Unexpected method call to onSuccess");
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("Unexpected method call", e);
            }
        }, r -> nextActionCalled.set(true), r2 -> {throw new AssertionError("unexpected call to action");});

        assertEquals(true, nextActionCalled.get());
        Mockito.verify(client, Mockito.only()).admin();
        Mockito.verify(adminClient, Mockito.only()).indices();
        Mockito.verify(indicesClient, Mockito.only()).segments(any(), any());
    }

    public void testCheckSegmentsNoNeedToMerge() {
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        Client client = Mockito.mock(Client.class);
        AdminClient adminClient = Mockito.mock(AdminClient.class);
        IndicesAdminClient indicesClient = Mockito.mock(IndicesAdminClient.class);
        IndicesSegmentResponse indicesSegmentResponse = Mockito.mock(IndicesSegmentResponse.class);
        IndexSegments indexSegments = Mockito.mock(IndexSegments.class);
        IndexShardSegments indexShardSegments = Mockito.mock(IndexShardSegments.class);
        Map<Integer, IndexShardSegments> indexShards = Collections.singletonMap(0, indexShardSegments);
        ShardSegments shardSegmentsOne = Mockito.mock(ShardSegments.class);
        ShardSegments[] shardSegmentsArray = new ShardSegments[] { shardSegmentsOne };
        Spliterator<IndexShardSegments> iss = indexShards.values().spliterator();
        List<Segment> segments = Arrays.asList(null, null);
        Mockito.when(indicesSegmentResponse.getStatus()).thenReturn(RestStatus.OK);
        Mockito.when(indicesSegmentResponse.getIndices()).thenReturn(Collections.singletonMap(index.getName(), indexSegments));
        Mockito.when(indexSegments.spliterator()).thenReturn(iss);
        Mockito.when(indexShardSegments.getShards()).thenReturn(shardSegmentsArray);
        Mockito.when(shardSegmentsOne.getSegments()).thenReturn(segments);
        int maxNumSegments = randomIntBetween(2, Integer.MAX_VALUE);

        Mockito.when(client.admin()).thenReturn(adminClient);
        Mockito.when(adminClient.indices()).thenReturn(indicesClient);

        Mockito.doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<IndicesSegmentResponse> listener = (ActionListener<IndicesSegmentResponse>) invocationOnMock.getArguments()[1];
            listener.onResponse(indicesSegmentResponse);
            return null;
        }).when(indicesClient).segments(any(), any());

        SetOnce<Boolean> skipActionCalled = new SetOnce<>();
        ForceMergeAction action = new ForceMergeAction(maxNumSegments);
        action.checkSegments(index, client, new Listener() {

            @Override
            public void onSuccess(boolean completed) {
                throw new AssertionError("Unexpected method call to onSuccess");
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("Unexpected method call", e);
            }
        }, r -> { throw new AssertionError("next action should not be called"); },
            r2 -> skipActionCalled.set(true));

        assertTrue(skipActionCalled.get());
        Mockito.verify(client, Mockito.only()).admin();
        Mockito.verify(adminClient, Mockito.only()).indices();
        Mockito.verify(indicesClient, Mockito.only()).segments(any(), any());
    }
}
