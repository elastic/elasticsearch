/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockUtils;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.XPackFeatureUsage;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.inference.InferenceFeatureSetUsage;
import org.elasticsearch.xpack.core.inference.action.GetInferenceModelAction;
import org.elasticsearch.xpack.core.watcher.support.xcontent.XContentSource;
import org.junit.After;
import org.junit.Before;

import java.util.List;

import static org.elasticsearch.xpack.inference.Utils.TIMEOUT;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.Is.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportInferenceUsageActionTests extends ESTestCase {

    private Client client;
    private TransportInferenceUsageAction action;

    @Before
    public void init() {
        client = mock(Client.class);
        ThreadPool threadPool = new TestThreadPool("test");
        when(client.threadPool()).thenReturn(threadPool);

        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor(mock(ThreadPool.class));

        action = new TransportInferenceUsageAction(
            transportService,
            mock(ClusterService.class),
            mock(ThreadPool.class),
            mock(ActionFilters.class),
            client
        );
    }

    @After
    public void close() {
        client.threadPool().shutdown();
    }

    public void test() throws Exception {
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<GetInferenceModelAction.Response>) invocation.getArguments()[2];
            listener.onResponse(
                new GetInferenceModelAction.Response(
                    List.of(
                        new ModelConfigurations("model-001", TaskType.TEXT_EMBEDDING, "openai", mock(ServiceSettings.class)),
                        new ModelConfigurations("model-002", TaskType.TEXT_EMBEDDING, "openai", mock(ServiceSettings.class)),
                        new ModelConfigurations("model-003", TaskType.SPARSE_EMBEDDING, "hugging_face_elser", mock(ServiceSettings.class)),
                        new ModelConfigurations("model-004", TaskType.TEXT_EMBEDDING, "openai", mock(ServiceSettings.class)),
                        new ModelConfigurations("model-005", TaskType.SPARSE_EMBEDDING, "openai", mock(ServiceSettings.class)),
                        new ModelConfigurations("model-006", TaskType.SPARSE_EMBEDDING, "hugging_face_elser", mock(ServiceSettings.class))
                    )
                )
            );
            return Void.TYPE;
        }).when(client).execute(any(GetInferenceModelAction.class), any(), any());

        PlainActionFuture<XPackUsageFeatureResponse> future = new PlainActionFuture<>();
        action.localClusterStateOperation(mock(Task.class), mock(XPackUsageRequest.class), mock(ClusterState.class), future);

        BytesStreamOutput out = new BytesStreamOutput();
        future.get().getUsage().writeTo(out);
        XPackFeatureUsage usage = new InferenceFeatureSetUsage(out.bytes().streamInput());

        assertThat(usage.name(), is(XPackField.INFERENCE));
        assertTrue(usage.enabled());
        assertTrue(usage.available());

        XContentBuilder builder = XContentFactory.jsonBuilder();
        usage.toXContent(builder, ToXContent.EMPTY_PARAMS);
        XContentSource source = new XContentSource(builder);
        assertThat(source.getValue("models"), hasSize(3));
        assertThat(source.getValue("models.0.service"), is("hugging_face_elser"));
        assertThat(source.getValue("models.0.task_type"), is("SPARSE_EMBEDDING"));
        assertThat(source.getValue("models.0.count"), is(2));
        assertThat(source.getValue("models.1.service"), is("openai"));
        assertThat(source.getValue("models.1.task_type"), is("SPARSE_EMBEDDING"));
        assertThat(source.getValue("models.1.count"), is(1));
        assertThat(source.getValue("models.2.service"), is("openai"));
        assertThat(source.getValue("models.2.task_type"), is("TEXT_EMBEDDING"));
        assertThat(source.getValue("models.2.count"), is(3));
    }

    public void testFailureReturnsEmptyUsage() {
        doAnswer(invocation -> {
            ActionListener<GetInferenceModelAction.Response> listener = invocation.getArgument(2);
            listener.onFailure(new IllegalArgumentException("invalid field"));
            return Void.TYPE;
        }).when(client).execute(any(GetInferenceModelAction.class), any(), any());

        var future = new PlainActionFuture<XPackUsageFeatureResponse>();
        action.localClusterStateOperation(mock(Task.class), mock(XPackUsageRequest.class), mock(ClusterState.class), future);

        var usage = future.actionGet(TIMEOUT);
        var inferenceUsage = (InferenceFeatureSetUsage) usage.getUsage();
        assertThat(inferenceUsage, is(InferenceFeatureSetUsage.EMPTY));
    }
}
