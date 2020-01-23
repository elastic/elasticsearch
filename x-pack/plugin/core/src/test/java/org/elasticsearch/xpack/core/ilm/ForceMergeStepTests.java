/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;


import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;
import org.mockito.Mockito;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;

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

        return new ForceMergeStep(key, nextKey, instance.getClient(), maxNumSegments);
    }

    @Override
    public ForceMergeStep copyInstance(ForceMergeStep instance) {
        return new ForceMergeStep(instance.getKey(), instance.getNextStepKey(),
            instance.getClient(), instance.getMaxNumSegments());
    }

    public void testPerformActionComplete() {
        IndexMetaData indexMetaData = IndexMetaData.builder(randomAlphaOfLength(10)).settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
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
        SetOnce<Boolean> completed = new SetOnce<>();
        step.performAction(indexMetaData, null, null, new AsyncActionStep.Listener() {
            @Override
            public void onResponse(boolean complete) {
                completed.set(complete);
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("unexpected method call", e);
            }
        });
        assertThat(completed.get(), equalTo(true));
    }

    public void testPerformActionThrowsException() {
        IndexMetaData indexMetaData = IndexMetaData.builder(randomAlphaOfLength(10)).settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        Exception exception = new RuntimeException("error");
        Step.StepKey stepKey = randomStepKey();
        StepKey nextStepKey = randomStepKey();
        int maxNumSegments = randomIntBetween(1, 10);
        ForceMergeResponse forceMergeResponse = Mockito.mock(ForceMergeResponse.class);
        Mockito.when(forceMergeResponse.getStatus()).thenReturn(RestStatus.OK);
        Mockito.doAnswer(invocationOnMock -> {
            ForceMergeRequest request = (ForceMergeRequest) invocationOnMock.getArguments()[0];
            assertThat(request.indices().length, equalTo(1));
            assertThat(request.indices()[0], equalTo(indexMetaData.getIndex().getName()));
            assertThat(request.maxNumSegments(), equalTo(maxNumSegments));
            @SuppressWarnings("unchecked")
            ActionListener<ForceMergeResponse> listener = (ActionListener<ForceMergeResponse>) invocationOnMock.getArguments()[1];
            listener.onFailure(exception);
            return null;
        }).when(indicesClient).forceMerge(any(), any());

        ForceMergeStep step = new ForceMergeStep(stepKey, nextStepKey, client, maxNumSegments);
        SetOnce<Boolean> exceptionThrown = new SetOnce<>();
        step.performAction(indexMetaData, null, null, new AsyncActionStep.Listener() {
            @Override
            public void onResponse(boolean complete) {
                throw new AssertionError("unexpected method call");
            }

            @Override
            public void onFailure(Exception e) {
                assertEquals(exception, e);
                exceptionThrown.set(true);
            }
        });
        assertThat(exceptionThrown.get(), equalTo(true));
    }
}
