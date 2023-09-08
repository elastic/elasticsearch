/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;
import org.mockito.Mockito;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;

public class ForceMergeStepTests extends AbstractStepTestCase<ForceMergeStep> {

    @Override
    public ForceMergeStep createRandomInstance() {
        Step.StepKey stepKey = randomStepKey();
        StepKey nextStepKey = randomStepKey();
        int maxNumSegments = randomIntBetween(1, 10);

        return new ForceMergeStep(stepKey, nextStepKey, null, maxNumSegments);
    }

    @Override
    public ForceMergeStep mutateInstance(ForceMergeStep instance) {
        StepKey key = instance.getKey();
        StepKey nextKey = instance.getNextStepKey();
        int maxNumSegments = instance.getMaxNumSegments();

        switch (between(0, 2)) {
            case 0 -> key = new StepKey(key.phase(), key.action(), key.name() + randomAlphaOfLength(5));
            case 1 -> nextKey = new StepKey(nextKey.phase(), nextKey.action(), nextKey.name() + randomAlphaOfLength(5));
            case 2 -> maxNumSegments += 1;
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new ForceMergeStep(key, nextKey, instance.getClient(), maxNumSegments);
    }

    @Override
    public ForceMergeStep copyInstance(ForceMergeStep instance) {
        return new ForceMergeStep(instance.getKey(), instance.getNextStepKey(), instance.getClient(), instance.getMaxNumSegments());
    }

    public void testPerformActionComplete() throws Exception {
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10))
            .settings(settings(IndexVersion.current()))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        Step.StepKey stepKey = randomStepKey();
        StepKey nextStepKey = randomStepKey();
        int maxNumSegments = randomIntBetween(1, 10);
        ForceMergeResponse forceMergeResponse = Mockito.mock(ForceMergeResponse.class);
        Mockito.when(forceMergeResponse.getStatus()).thenReturn(RestStatus.OK);
        Mockito.doAnswer(invocationOnMock -> {
            ForceMergeRequest request = (ForceMergeRequest) invocationOnMock.getArguments()[0];
            assertThat(request.maxNumSegments(), equalTo(maxNumSegments));
            @SuppressWarnings("unchecked")
            ActionListener<ForceMergeResponse> listener = (ActionListener<ForceMergeResponse>) invocationOnMock.getArguments()[1];
            listener.onResponse(forceMergeResponse);
            return null;
        }).when(indicesClient).forceMerge(any(), any());

        ForceMergeStep step = new ForceMergeStep(stepKey, nextStepKey, client, maxNumSegments);
        PlainActionFuture.<Void, Exception>get(f -> step.performAction(indexMetadata, null, null, f));
    }

    public void testPerformActionThrowsException() {
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10))
            .settings(settings(IndexVersion.current()))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        Exception exception = new RuntimeException("error");
        Step.StepKey stepKey = randomStepKey();
        StepKey nextStepKey = randomStepKey();
        int maxNumSegments = randomIntBetween(1, 10);
        ForceMergeResponse forceMergeResponse = Mockito.mock(ForceMergeResponse.class);
        Mockito.when(forceMergeResponse.getStatus()).thenReturn(RestStatus.OK);
        Mockito.doAnswer(invocationOnMock -> {
            ForceMergeRequest request = (ForceMergeRequest) invocationOnMock.getArguments()[0];
            assertThat(request.indices().length, equalTo(1));
            assertThat(request.indices()[0], equalTo(indexMetadata.getIndex().getName()));
            assertThat(request.maxNumSegments(), equalTo(maxNumSegments));
            @SuppressWarnings("unchecked")
            ActionListener<ForceMergeResponse> listener = (ActionListener<ForceMergeResponse>) invocationOnMock.getArguments()[1];
            listener.onFailure(exception);
            return null;
        }).when(indicesClient).forceMerge(any(), any());

        ForceMergeStep step = new ForceMergeStep(stepKey, nextStepKey, client, maxNumSegments);
        assertSame(
            exception,
            expectThrows(
                Exception.class,
                () -> PlainActionFuture.<Void, Exception>get(f -> step.performAction(indexMetadata, null, null, f))
            )
        );
    }

    public void testForcemergeFailsOnSomeShards() {
        int numberOfShards = randomIntBetween(2, 5);
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10))
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, "ilmPolicy"))
            .numberOfShards(numberOfShards)
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        Index index = indexMetadata.getIndex();
        ForceMergeResponse forceMergeResponse = Mockito.mock(ForceMergeResponse.class);
        Mockito.when(forceMergeResponse.getTotalShards()).thenReturn(numberOfShards);
        Mockito.when(forceMergeResponse.getFailedShards()).thenReturn(numberOfShards - 1);
        Mockito.when(forceMergeResponse.getStatus()).thenReturn(RestStatus.BAD_REQUEST);
        Mockito.when(forceMergeResponse.getSuccessfulShards()).thenReturn(1);
        DefaultShardOperationFailedException cause = new DefaultShardOperationFailedException(
            index.getName(),
            0,
            new IllegalArgumentException("couldn't merge")
        );
        Mockito.when(forceMergeResponse.getShardFailures()).thenReturn(new DefaultShardOperationFailedException[] { cause });

        Step.StepKey stepKey = randomStepKey();
        StepKey nextStepKey = randomStepKey();

        Mockito.doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<ForceMergeResponse> listener = (ActionListener<ForceMergeResponse>) invocationOnMock.getArguments()[1];
            listener.onResponse(forceMergeResponse);
            return null;
        }).when(indicesClient).forceMerge(any(), any());

        SetOnce<ElasticsearchException> failedStep = new SetOnce<>();

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(indexMetadata, true).build())
            .build();
        ForceMergeStep step = new ForceMergeStep(stepKey, nextStepKey, client, 1);
        step.performAction(indexMetadata, state, null, new ActionListener<>() {
            @Override
            public void onResponse(Void aBoolean) {
                throw new AssertionError("unexpected method call [onResponse]. expecting [onFailure]");
            }

            @Override
            public void onFailure(Exception e) {
                assert e instanceof ElasticsearchException
                    : "step must report " + ElasticsearchException.class.getSimpleName() + " but was " + e;
                failedStep.set((ElasticsearchException) e);
            }
        });

        ElasticsearchException stepException = failedStep.get();
        assertThat(stepException, notNullValue());
        assertThat(stepException.getMessage(), is(Strings.format("""
            index [%s] in policy [ilmPolicy] encountered failures [{"shard":0,"index":"%s","status":"BAD_REQUEST",\
            "reason":{"type":"illegal_argument_exception","reason":"couldn't merge"}}] on step [forcemerge]\
            """, index.getName(), index.getName())));
    }
}
