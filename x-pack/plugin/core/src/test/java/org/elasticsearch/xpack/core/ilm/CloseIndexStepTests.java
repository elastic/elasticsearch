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
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.AdminClient;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.IndicesAdminClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.index.IndexVersion;
import org.junit.Before;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

public class CloseIndexStepTests extends AbstractStepTestCase<CloseIndexStep> {

    private Client client;

    @Before
    public void setup() {
        client = Mockito.mock(Client.class);
    }

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

        CloseIndexStep step = createRandomInstance();

        AdminClient adminClient = Mockito.mock(AdminClient.class);
        IndicesAdminClient indicesClient = Mockito.mock(IndicesAdminClient.class);

        Mockito.when(client.admin()).thenReturn(adminClient);
        Mockito.when(adminClient.indices()).thenReturn(indicesClient);

        Mockito.doAnswer((Answer<Void>) invocation -> {
            CloseIndexRequest request = (CloseIndexRequest) invocation.getArguments()[0];
            @SuppressWarnings("unchecked")
            ActionListener<CloseIndexResponse> listener = (ActionListener<CloseIndexResponse>) invocation.getArguments()[1];
            assertThat(request.indices(), equalTo(new String[] { indexMetadata.getIndex().getName() }));
            listener.onResponse(
                new CloseIndexResponse(true, true, Collections.singletonList(new CloseIndexResponse.IndexResult(indexMetadata.getIndex())))
            );
            return null;
        }).when(indicesClient).close(Mockito.any(), Mockito.any());

        SetOnce<Boolean> actionCompleted = new SetOnce<>();

        step.performAction(indexMetadata, null, null, new ActionListener<>() {

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
        Mockito.verify(client, Mockito.only()).admin();
        Mockito.verify(adminClient, Mockito.only()).indices();
        Mockito.verify(indicesClient, Mockito.only()).close(Mockito.any(), Mockito.any());
    }

    public void testPerformActionFailure() {
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10))
            .settings(settings(IndexVersion.current()))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        CloseIndexStep step = createRandomInstance();
        Exception exception = new RuntimeException();
        AdminClient adminClient = Mockito.mock(AdminClient.class);
        IndicesAdminClient indicesClient = Mockito.mock(IndicesAdminClient.class);

        Mockito.when(client.admin()).thenReturn(adminClient);
        Mockito.when(adminClient.indices()).thenReturn(indicesClient);

        Mockito.doAnswer((Answer<Void>) invocation -> {
            CloseIndexRequest request = (CloseIndexRequest) invocation.getArguments()[0];
            @SuppressWarnings("unchecked")
            ActionListener<CloseIndexResponse> listener = (ActionListener<CloseIndexResponse>) invocation.getArguments()[1];
            assertThat(request.indices(), equalTo(new String[] { indexMetadata.getIndex().getName() }));
            listener.onFailure(exception);
            return null;
        }).when(indicesClient).close(Mockito.any(), Mockito.any());

        assertSame(
            exception,
            expectThrows(
                Exception.class,
                () -> PlainActionFuture.<Void, Exception>get(f -> step.performAction(indexMetadata, null, null, f))
            )
        );

        Mockito.verify(client, Mockito.only()).admin();
        Mockito.verify(adminClient, Mockito.only()).indices();
        Mockito.verify(indicesClient, Mockito.only()).close(Mockito.any(), Mockito.any());
    }
}
