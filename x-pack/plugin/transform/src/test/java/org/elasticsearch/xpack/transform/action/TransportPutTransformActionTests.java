/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.internal.AdminClient;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.IndicesAdminClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.transform.TransformMessages;
import org.elasticsearch.xpack.transform.persistence.TransformInternalIndex;

import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportPutTransformActionTests extends ESTestCase {

    public void testIndexCreationFailureReturns503() {
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);

        IndicesAdminClient indicesClient = mock(IndicesAdminClient.class);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<CreateIndexResponse> listener = (ActionListener<CreateIndexResponse>) invocationOnMock.getArguments()[1];
            listener.onFailure(new RuntimeException("simulated index creation failure"));
            return null;
        }).when(indicesClient).create(any(), any());

        AdminClient adminClient = mock(AdminClient.class);
        when(adminClient.indices()).thenReturn(indicesClient);
        Client client = mock(Client.class);
        when(client.admin()).thenReturn(adminClient);

        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(client.threadPool()).thenReturn(threadPool);

        AtomicReference<Exception> capturedException = new AtomicReference<>();
        ActionListener<Void> listener = ActionListener.<Void>wrap(r -> fail("expected failure but got success"), capturedException::set);

        // Replicate the delegateResponse wrapping from TransportPutTransformAction
        TransformInternalIndex.createLatestVersionedIndexIfRequired(
            clusterService,
            client,
            Settings.EMPTY,
            listener.delegateResponse((l, e) -> {
                l.onFailure(
                    new ElasticsearchStatusException(
                        TransformMessages.REST_PUT_FAILED_CREATING_TRANSFORM_INDEX,
                        RestStatus.SERVICE_UNAVAILABLE,
                        ExceptionsHelper.unwrapCause(e)
                    )
                );
            })
        );

        assertThat(capturedException.get(), notNullValue());
        assertThat(capturedException.get(), instanceOf(ElasticsearchStatusException.class));
        ElasticsearchStatusException statusException = (ElasticsearchStatusException) capturedException.get();
        assertThat(statusException.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        assertThat(statusException.getMessage(), equalTo(TransformMessages.REST_PUT_FAILED_CREATING_TRANSFORM_INDEX));
    }
}
