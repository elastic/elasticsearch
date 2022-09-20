/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.AdminClient;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.IndicesAdminClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.junit.Before;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import static org.hamcrest.Matchers.equalTo;

public class OpenIndexStepTests extends AbstractStepTestCase<OpenIndexStep> {

    private Client client;

    @Before
    public void setup() {
        client = Mockito.mock(Client.class);
    }

    @Override
    protected OpenIndexStep createRandomInstance() {
        return new OpenIndexStep(randomStepKey(), randomStepKey(), client);
    }

    @Override
    protected OpenIndexStep mutateInstance(OpenIndexStep instance) {
        Step.StepKey key = instance.getKey();
        Step.StepKey nextKey = instance.getNextStepKey();

        switch (between(0, 1)) {
            case 0 -> key = new Step.StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
            case 1 -> nextKey = new Step.StepKey(nextKey.getPhase(), nextKey.getAction(), nextKey.getName() + randomAlphaOfLength(5));
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
            .settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .state(IndexMetadata.State.CLOSE)
            .build();

        OpenIndexStep step = createRandomInstance();

        AdminClient adminClient = Mockito.mock(AdminClient.class);
        IndicesAdminClient indicesClient = Mockito.mock(IndicesAdminClient.class);

        Mockito.when(client.admin()).thenReturn(adminClient);
        Mockito.when(adminClient.indices()).thenReturn(indicesClient);

        Mockito.doAnswer((Answer<Void>) invocation -> {
            OpenIndexRequest request = (OpenIndexRequest) invocation.getArguments()[0];
            @SuppressWarnings("unchecked")
            ActionListener<OpenIndexResponse> listener = (ActionListener<OpenIndexResponse>) invocation.getArguments()[1];
            assertThat(request.indices(), equalTo(new String[] { indexMetadata.getIndex().getName() }));
            listener.onResponse(new OpenIndexResponse(true, true));
            return null;
        }).when(indicesClient).open(Mockito.any(), Mockito.any());

        PlainActionFuture.<Void, Exception>get(f -> step.performAction(indexMetadata, null, null, f));

        Mockito.verify(client, Mockito.only()).admin();
        Mockito.verify(adminClient, Mockito.only()).indices();
        Mockito.verify(indicesClient, Mockito.only()).open(Mockito.any(), Mockito.any());
    }

    public void testPerformActionFailure() {
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10))
            .settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .state(IndexMetadata.State.CLOSE)
            .build();

        OpenIndexStep step = createRandomInstance();
        Exception exception = new RuntimeException();
        AdminClient adminClient = Mockito.mock(AdminClient.class);
        IndicesAdminClient indicesClient = Mockito.mock(IndicesAdminClient.class);

        Mockito.when(client.admin()).thenReturn(adminClient);
        Mockito.when(adminClient.indices()).thenReturn(indicesClient);

        Mockito.doAnswer((Answer<Void>) invocation -> {
            OpenIndexRequest request = (OpenIndexRequest) invocation.getArguments()[0];
            @SuppressWarnings("unchecked")
            ActionListener<OpenIndexResponse> listener = (ActionListener<OpenIndexResponse>) invocation.getArguments()[1];
            assertThat(request.indices(), equalTo(new String[] { indexMetadata.getIndex().getName() }));
            listener.onFailure(exception);
            return null;
        }).when(indicesClient).open(Mockito.any(), Mockito.any());

        assertSame(
            exception,
            expectThrows(
                Exception.class,
                () -> PlainActionFuture.<Void, Exception>get(f -> step.performAction(indexMetadata, null, null, f))
            )
        );

        Mockito.verify(client, Mockito.only()).admin();
        Mockito.verify(adminClient, Mockito.only()).indices();
        Mockito.verify(indicesClient, Mockito.only()).open(Mockito.any(), Mockito.any());
    }
}
