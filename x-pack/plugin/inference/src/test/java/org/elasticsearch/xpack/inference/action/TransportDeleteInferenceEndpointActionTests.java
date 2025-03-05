/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.inference.action.DeleteInferenceEndpointAction;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.junit.After;
import org.junit.Before;

import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class TransportDeleteInferenceEndpointActionTests extends ESTestCase {

    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(30);

    private TransportDeleteInferenceEndpointAction action;
    private ThreadPool threadPool;
    private ModelRegistry modelRegistry;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        modelRegistry = new ModelRegistry(mock(Client.class));
        threadPool = createThreadPool(inferenceUtilityPool());
        action = new TransportDeleteInferenceEndpointAction(
            mock(TransportService.class),
            mock(ClusterService.class),
            threadPool,
            mock(ActionFilters.class),
            modelRegistry,
            mock(InferenceServiceRegistry.class)
        );
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        terminate(threadPool);
    }

    public void testFailsToDelete_ADefaultEndpoint() {
        modelRegistry.addDefaultIds(
            new InferenceService.DefaultConfigId("model-id", MinimalServiceSettings.chatCompletion(), mock(InferenceService.class))
        );

        var listener = new PlainActionFuture<DeleteInferenceEndpointAction.Response>();

        action.masterOperation(
            mock(Task.class),
            new DeleteInferenceEndpointAction.Request("model-id", TaskType.CHAT_COMPLETION, true, false),
            mock(ClusterState.class),
            listener
        );

        var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(
            exception.getMessage(),
            is("[model-id] is a reserved inference endpoint. " + "Cannot delete a reserved inference endpoint.")
        );
    }
}
