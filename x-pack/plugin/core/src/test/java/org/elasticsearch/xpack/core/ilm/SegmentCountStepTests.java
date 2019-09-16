/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.segments.IndexSegments;
import org.elasticsearch.action.admin.indices.segments.IndexShardSegments;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.elasticsearch.action.admin.indices.segments.ShardSegments;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.engine.Segment;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;

public class SegmentCountStepTests extends AbstractStepTestCase<SegmentCountStep> {

    @Override
    public SegmentCountStep createRandomInstance() {
        Step.StepKey stepKey = randomStepKey();
        StepKey nextStepKey = randomStepKey();
        int maxNumSegments = randomIntBetween(1, 10);

        return new SegmentCountStep(stepKey, nextStepKey, null, maxNumSegments);
    }

    private IndexMetaData makeMeta(Index index) {
        return IndexMetaData.builder(index.getName())
            .settings(Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT))
            .build();
    }

    @Override
    public SegmentCountStep mutateInstance(SegmentCountStep instance) {
        StepKey key = instance.getKey();
        StepKey nextKey = instance.getNextStepKey();
        int maxNumSegments = instance.getMaxNumSegments();

        switch (between(0, 2)) {
            case 0:
                key = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
                break;
            case 1:
                nextKey = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
                break;
            case 2:
                maxNumSegments += 1;
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
        }

        return new SegmentCountStep(key, nextKey, null, maxNumSegments);
    }

    @Override
    public SegmentCountStep copyInstance(SegmentCountStep instance) {
        return new SegmentCountStep(instance.getKey(), instance.getNextStepKey(), null, instance.getMaxNumSegments());
    }

    public void testIsConditionMet() {
        int maxNumSegments = randomIntBetween(3, 10);
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
        List<Segment> segments = new ArrayList<>();
        for (int i = 0; i < maxNumSegments - randomIntBetween(0, 3); i++) {
            segments.add(null);
        }
        Mockito.when(indicesSegmentResponse.getStatus()).thenReturn(RestStatus.OK);
        Mockito.when(indicesSegmentResponse.getIndices()).thenReturn(Collections.singletonMap(index.getName(), indexSegments));
        Mockito.when(indexSegments.spliterator()).thenReturn(iss);
        Mockito.when(indexShardSegments.getShards()).thenReturn(shardSegmentsArray);
        Mockito.when(shardSegmentsOne.getSegments()).thenReturn(segments);

        Mockito.when(client.admin()).thenReturn(adminClient);
        Mockito.when(adminClient.indices()).thenReturn(indicesClient);

        Step.StepKey stepKey = randomStepKey();
        StepKey nextStepKey = randomStepKey();

        Mockito.doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<IndicesSegmentResponse> listener = (ActionListener<IndicesSegmentResponse>) invocationOnMock.getArguments()[1];
            listener.onResponse(indicesSegmentResponse);
            return null;
        }).when(indicesClient).segments(any(), any());

        SetOnce<Boolean> conditionMetResult = new SetOnce<>();
        SetOnce<ToXContentObject> conditionInfo = new SetOnce<>();

        SegmentCountStep step = new SegmentCountStep(stepKey, nextStepKey, client, maxNumSegments);
        step.evaluateCondition(makeMeta(index), new AsyncWaitStep.Listener() {
            @Override
            public void onResponse(boolean conditionMet, ToXContentObject info) {
                conditionMetResult.set(conditionMet);
                conditionInfo.set(info);
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("unexpected onFailure call", e);
                throw new AssertionError("unexpected method call");
            }
        });

        assertTrue(conditionMetResult.get());
        assertEquals(new SegmentCountStep.Info(0L), conditionInfo.get());
    }

    public void testIsConditionIsTrueEvenWhenMoreSegments() {
        int maxNumSegments = randomIntBetween(3, 10);
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
        List<Segment> segments = new ArrayList<>();
        for (int i = 0; i < maxNumSegments + randomIntBetween(1, 3); i++) {
            segments.add(null);
        }
        Mockito.when(indicesSegmentResponse.getStatus()).thenReturn(RestStatus.OK);
        Mockito.when(indicesSegmentResponse.getIndices()).thenReturn(Collections.singletonMap(index.getName(), indexSegments));
        Mockito.when(indexSegments.spliterator()).thenReturn(iss);
        Mockito.when(indexShardSegments.getShards()).thenReturn(shardSegmentsArray);
        Mockito.when(shardSegmentsOne.getSegments()).thenReturn(segments);

        Mockito.when(client.admin()).thenReturn(adminClient);
        Mockito.when(adminClient.indices()).thenReturn(indicesClient);

        Step.StepKey stepKey = randomStepKey();
        StepKey nextStepKey = randomStepKey();

        Mockito.doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<IndicesSegmentResponse> listener = (ActionListener<IndicesSegmentResponse>) invocationOnMock.getArguments()[1];
            listener.onResponse(indicesSegmentResponse);
            return null;
        }).when(indicesClient).segments(any(), any());

        SetOnce<Boolean> conditionMetResult = new SetOnce<>();
        SetOnce<ToXContentObject> conditionInfo = new SetOnce<>();

        SegmentCountStep step = new SegmentCountStep(stepKey, nextStepKey, client, maxNumSegments);
        step.evaluateCondition(makeMeta(index), new AsyncWaitStep.Listener() {
            @Override
            public void onResponse(boolean conditionMet, ToXContentObject info) {
                conditionMetResult.set(conditionMet);
                conditionInfo.set(info);
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("unexpected onFailure call", e);
                throw new AssertionError("unexpected method call");
            }
        });

        assertTrue(conditionMetResult.get());
        assertEquals(new SegmentCountStep.Info(0L), conditionInfo.get());
    }

    public void testFailedToRetrieveSomeSegments() {
        int maxNumSegments = randomIntBetween(3, 10);
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
        List<Segment> segments = new ArrayList<>();
        for (int i = 0; i < maxNumSegments + randomIntBetween(1, 3); i++) {
            segments.add(null);
        }
        Mockito.when(indicesSegmentResponse.getStatus()).thenReturn(RestStatus.OK);
        Mockito.when(indicesSegmentResponse.getIndices()).thenReturn(Collections.singletonMap(index.getName(), null));
        Mockito.when(indicesSegmentResponse.getShardFailures())
            .thenReturn(new DefaultShardOperationFailedException[]{new DefaultShardOperationFailedException(index.getName(),
                0, new IllegalArgumentException("fake"))});
        Mockito.when(indexSegments.spliterator()).thenReturn(iss);
        Mockito.when(indexShardSegments.getShards()).thenReturn(shardSegmentsArray);
        Mockito.when(shardSegmentsOne.getSegments()).thenReturn(segments);

        Mockito.when(client.admin()).thenReturn(adminClient);
        Mockito.when(adminClient.indices()).thenReturn(indicesClient);

        Step.StepKey stepKey = randomStepKey();
        StepKey nextStepKey = randomStepKey();

        Mockito.doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<IndicesSegmentResponse> listener = (ActionListener<IndicesSegmentResponse>) invocationOnMock.getArguments()[1];
            listener.onResponse(indicesSegmentResponse);
            return null;
        }).when(indicesClient).segments(any(), any());

        SetOnce<Boolean> conditionMetResult = new SetOnce<>();
        SetOnce<ToXContentObject> conditionInfo = new SetOnce<>();

        SegmentCountStep step = new SegmentCountStep(stepKey, nextStepKey, client, maxNumSegments);
        step.evaluateCondition(makeMeta(index), new AsyncWaitStep.Listener() {
            @Override
            public void onResponse(boolean conditionMet, ToXContentObject info) {
                conditionMetResult.set(conditionMet);
                conditionInfo.set(info);
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("unexpected onFailure call", e);
                throw new AssertionError("unexpected method call: " + e);
            }
        });

        assertTrue(conditionMetResult.get());
        assertEquals(new SegmentCountStep.Info(-1L), conditionInfo.get());
    }

    public void testThrowsException() {
        Exception exception = new RuntimeException("error");
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        Client client = Mockito.mock(Client.class);
        AdminClient adminClient = Mockito.mock(AdminClient.class);
        IndicesAdminClient indicesClient = Mockito.mock(IndicesAdminClient.class);
        Mockito.when(client.admin()).thenReturn(adminClient);
        Mockito.when(adminClient.indices()).thenReturn(indicesClient);

        Step.StepKey stepKey = randomStepKey();
        StepKey nextStepKey = randomStepKey();
        int maxNumSegments = randomIntBetween(3, 10);

        Mockito.doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<IndicesSegmentResponse> listener = (ActionListener<IndicesSegmentResponse>) invocationOnMock.getArguments()[1];
            listener.onFailure(exception);
            return null;
        }).when(indicesClient).segments(any(), any());

        SetOnce<Boolean> exceptionThrown = new SetOnce<>();

        SegmentCountStep step = new SegmentCountStep(stepKey, nextStepKey, client, maxNumSegments);
        step.evaluateCondition(makeMeta(index), new AsyncWaitStep.Listener() {
            @Override
            public void onResponse(boolean conditionMet, ToXContentObject info) {
                throw new AssertionError("unexpected method call");
            }

            @Override
            public void onFailure(Exception e) {
                assertThat(e, equalTo(exception));
                exceptionThrown.set(true);
            }
        });

        assertTrue(exceptionThrown.get());
    }
}
