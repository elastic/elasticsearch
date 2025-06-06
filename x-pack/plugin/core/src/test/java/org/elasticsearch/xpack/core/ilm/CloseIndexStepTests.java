/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.index.IndexVersion;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class CloseIndexStepTests extends AbstractStepTestCase<CloseIndexStep> {

    @Override
    protected CloseIndexStep createRandomInstance() {
        return new CloseIndexStep(randomStepKey(), randomStepKey(), client);
    }

    @Override
    protected CloseIndexStep mutateInstance(CloseIndexStep instance) {
        Step.StepKey key = instance.getKey();
        Step.StepKey nextKey = instance.getNextStepKey();

        switch (between(0, 1)) {
            case 0 -> key = new Step.StepKey(key.phase(), key.action(), key.name() + randomAlphaOfLength(5));
            case 1 -> nextKey = new Step.StepKey(nextKey.phase(), nextKey.action(), nextKey.name() + randomAlphaOfLength(5));
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new CloseIndexStep(key, nextKey, client);
    }

    @Override
    protected CloseIndexStep copyInstance(CloseIndexStep instance) {
        return new CloseIndexStep(instance.getKey(), instance.getNextStepKey(), instance.getClient());
    }

    public void testPerformAction() {
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10))
            .settings(settings(IndexVersion.current()))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        var state = projectStateWithEmptyProject();

        CloseIndexStep step = createRandomInstance();

        Mockito.doAnswer((Answer<Void>) invocation -> {
            CloseIndexRequest request = (CloseIndexRequest) invocation.getArguments()[0];
            @SuppressWarnings("unchecked")
            ActionListener<CloseIndexResponse> listener = (ActionListener<CloseIndexResponse>) invocation.getArguments()[1];
            assertThat(request.indices(), equalTo(new String[] { indexMetadata.getIndex().getName() }));
            listener.onResponse(new CloseIndexResponse(true, true, List.of(new CloseIndexResponse.IndexResult(indexMetadata.getIndex()))));
            return null;
        }).when(indicesClient).close(Mockito.any(), Mockito.any());

        SetOnce<Boolean> actionCompleted = new SetOnce<>();

        step.performAction(indexMetadata, state, null, new ActionListener<>() {

            @Override
            public void onResponse(Void complete) {
                actionCompleted.set(true);
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("Unexpected method call", e);
            }
        });

        assertEquals(true, actionCompleted.get());
        Mockito.verify(client).projectClient(state.projectId());
        Mockito.verify(client).admin();
        Mockito.verifyNoMoreInteractions(client);
        Mockito.verify(adminClient, Mockito.only()).indices();
        Mockito.verify(indicesClient, Mockito.only()).close(Mockito.any(), Mockito.any());
    }

    public void testPerformActionFailure() {
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10))
            .settings(settings(IndexVersion.current()))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        var state = projectStateWithEmptyProject();

        CloseIndexStep step = createRandomInstance();
        Exception exception = new RuntimeException();

        Mockito.doAnswer((Answer<Void>) invocation -> {
            CloseIndexRequest request = (CloseIndexRequest) invocation.getArguments()[0];
            @SuppressWarnings("unchecked")
            ActionListener<CloseIndexResponse> listener = (ActionListener<CloseIndexResponse>) invocation.getArguments()[1];
            assertThat(request.indices(), equalTo(new String[] { indexMetadata.getIndex().getName() }));
            listener.onFailure(exception);
            return null;
        }).when(indicesClient).close(Mockito.any(), Mockito.any());

        assertSame(exception, expectThrows(Exception.class, () -> performActionAndWait(step, indexMetadata, state, null)));
        Mockito.verify(client).projectClient(state.projectId());
        Mockito.verify(client).admin();
        Mockito.verifyNoMoreInteractions(client);
        Mockito.verify(adminClient, Mockito.only()).indices();
        Mockito.verify(indicesClient, Mockito.only()).close(Mockito.any(), Mockito.any());
    }
}
