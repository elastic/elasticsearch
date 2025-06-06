/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.index.IndexVersion;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import static org.hamcrest.Matchers.equalTo;

public class OpenIndexStepTests extends AbstractStepTestCase<OpenIndexStep> {

    @Override
    protected OpenIndexStep createRandomInstance() {
        return new OpenIndexStep(randomStepKey(), randomStepKey(), client);
    }

    @Override
    protected OpenIndexStep mutateInstance(OpenIndexStep instance) {
        Step.StepKey key = instance.getKey();
        Step.StepKey nextKey = instance.getNextStepKey();

        switch (between(0, 1)) {
            case 0 -> key = new Step.StepKey(key.phase(), key.action(), key.name() + randomAlphaOfLength(5));
            case 1 -> nextKey = new Step.StepKey(nextKey.phase(), nextKey.action(), nextKey.name() + randomAlphaOfLength(5));
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new OpenIndexStep(key, nextKey, client);
    }

    @Override
    protected OpenIndexStep copyInstance(OpenIndexStep instance) {
        return new OpenIndexStep(instance.getKey(), instance.getNextStepKey(), instance.getClient());
    }

    public void testPerformAction() throws Exception {
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10))
            .settings(settings(IndexVersion.current()))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .state(IndexMetadata.State.CLOSE)
            .build();

        OpenIndexStep step = createRandomInstance();

        Mockito.doAnswer((Answer<Void>) invocation -> {
            OpenIndexRequest request = (OpenIndexRequest) invocation.getArguments()[0];
            @SuppressWarnings("unchecked")
            ActionListener<OpenIndexResponse> listener = (ActionListener<OpenIndexResponse>) invocation.getArguments()[1];
            assertThat(request.indices(), equalTo(new String[] { indexMetadata.getIndex().getName() }));
            listener.onResponse(new OpenIndexResponse(true, true));
            return null;
        }).when(indicesClient).open(Mockito.any(), Mockito.any());

        var state = projectStateWithEmptyProject();
        performActionAndWait(step, indexMetadata, state, null);

        Mockito.verify(client).projectClient(state.projectId());
        Mockito.verify(client).admin();
        Mockito.verifyNoMoreInteractions(client);
        Mockito.verify(adminClient, Mockito.only()).indices();
        Mockito.verify(indicesClient, Mockito.only()).open(Mockito.any(), Mockito.any());
    }

    public void testPerformActionFailure() {
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10))
            .settings(settings(IndexVersion.current()))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .state(IndexMetadata.State.CLOSE)
            .build();

        OpenIndexStep step = createRandomInstance();
        Exception exception = new RuntimeException();

        Mockito.doAnswer((Answer<Void>) invocation -> {
            OpenIndexRequest request = (OpenIndexRequest) invocation.getArguments()[0];
            @SuppressWarnings("unchecked")
            ActionListener<OpenIndexResponse> listener = (ActionListener<OpenIndexResponse>) invocation.getArguments()[1];
            assertThat(request.indices(), equalTo(new String[] { indexMetadata.getIndex().getName() }));
            listener.onFailure(exception);
            return null;
        }).when(indicesClient).open(Mockito.any(), Mockito.any());

        var state = projectStateWithEmptyProject();
        assertSame(exception, expectThrows(Exception.class, () -> performActionAndWait(step, indexMetadata, state, null)));

        Mockito.verify(client).projectClient(state.projectId());
        Mockito.verify(client).admin();
        Mockito.verifyNoMoreInteractions(client);
        Mockito.verify(adminClient, Mockito.only()).indices();
        Mockito.verify(indicesClient, Mockito.only()).open(Mockito.any(), Mockito.any());
    }
}
