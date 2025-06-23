/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.inference.action.task.StreamingTaskManager;
import org.elasticsearch.xpack.inference.common.InferenceServiceRateLimitCalculator;
import org.elasticsearch.xpack.inference.common.RateLimitAssignment;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.elasticsearch.xpack.inference.telemetry.InferenceStats;

import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.assertArg;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportInferenceActionTests extends BaseTransportInferenceActionTestCase<InferenceAction.Request> {

    public TransportInferenceActionTests() {
        super(TaskType.COMPLETION);
    }

    @Override
    protected BaseTransportInferenceAction<InferenceAction.Request> createAction(
        TransportService transportService,
        ActionFilters actionFilters,
        MockLicenseState licenseState,
        ModelRegistry modelRegistry,
        InferenceServiceRegistry serviceRegistry,
        InferenceStats inferenceStats,
        StreamingTaskManager streamingTaskManager,
        InferenceServiceRateLimitCalculator inferenceServiceNodeLocalRateLimitCalculator,
        NodeClient nodeClient,
        ThreadPool threadPool
    ) {
        return new TransportInferenceAction(
            transportService,
            actionFilters,
            licenseState,
            modelRegistry,
            serviceRegistry,
            inferenceStats,
            streamingTaskManager,
            inferenceServiceNodeLocalRateLimitCalculator,
            nodeClient,
            threadPool
        );
    }

    @Override
    protected InferenceAction.Request createRequest() {
        return mock(InferenceAction.Request.class);
    }

    public void testNoRerouting_WhenTaskTypeNotSupported() {
        TaskType unsupportedTaskType = TaskType.COMPLETION;
        mockService(listener -> listener.onResponse(mock()));

        when(inferenceServiceRateLimitCalculator.isTaskTypeReroutingSupported(serviceId, unsupportedTaskType)).thenReturn(false);

        var listener = doExecute(unsupportedTaskType);

        verify(listener).onResponse(any());
        // Verify request was handled locally (not rerouted using TransportService)
        verify(transportService, never()).sendRequest(any(), any(), any(), any());
        // Verify request metric attributes were recorded on the node performing inference
        verify(inferenceStats.inferenceDuration()).record(anyLong(), assertArg(attributes -> {
            assertThat(attributes.get("rerouted"), is(Boolean.FALSE));
            assertThat(attributes.get("node_id"), is(localNodeId));
        }));
    }

    public void testNoRerouting_WhenNoGroupingCalculatedYet() {
        mockService(listener -> listener.onResponse(mock()));

        when(inferenceServiceRateLimitCalculator.isTaskTypeReroutingSupported(serviceId, taskType)).thenReturn(true);
        when(inferenceServiceRateLimitCalculator.getRateLimitAssignment(serviceId, taskType)).thenReturn(null);

        var listener = doExecute(taskType);

        verify(listener).onResponse(any());
        // Verify request was handled locally (not rerouted using TransportService)
        verify(transportService, never()).sendRequest(any(), any(), any(), any());
        // Verify request metric attributes were recorded on the node performing inference
        verify(inferenceStats.inferenceDuration()).record(anyLong(), assertArg(attributes -> {
            assertThat(attributes.get("rerouted"), is(Boolean.FALSE));
            assertThat(attributes.get("node_id"), is(localNodeId));
        }));
    }

    public void testNoRerouting_WhenEmptyNodeList() {
        mockService(listener -> listener.onResponse(mock()));

        when(inferenceServiceRateLimitCalculator.isTaskTypeReroutingSupported(serviceId, taskType)).thenReturn(true);
        when(inferenceServiceRateLimitCalculator.getRateLimitAssignment(serviceId, taskType)).thenReturn(
            new RateLimitAssignment(List.of())
        );

        var listener = doExecute(taskType);

        verify(listener).onResponse(any());
        // Verify request was handled locally (not rerouted using TransportService)
        verify(transportService, never()).sendRequest(any(), any(), any(), any());
        // Verify request metric attributes were recorded on the node performing inference
        verify(inferenceStats.inferenceDuration()).record(anyLong(), assertArg(attributes -> {
            assertThat(attributes.get("rerouted"), is(Boolean.FALSE));
            assertThat(attributes.get("node_id"), is(localNodeId));
        }));
    }

    public void testRerouting_ToOtherNode() {
        DiscoveryNode otherNode = mock(DiscoveryNode.class);
        when(otherNode.getId()).thenReturn("other-node");

        // The local node is different to the "other-node" responsible for serviceId
        when(nodeClient.getLocalNodeId()).thenReturn("local-node");
        when(inferenceServiceRateLimitCalculator.isTaskTypeReroutingSupported(serviceId, taskType)).thenReturn(true);
        // Requests for serviceId are always routed to "other-node"
        var assignment = new RateLimitAssignment(List.of(otherNode));
        when(inferenceServiceRateLimitCalculator.getRateLimitAssignment(serviceId, taskType)).thenReturn(assignment);

        mockService(listener -> listener.onResponse(mock()));
        var listener = doExecute(taskType);

        // Verify request was rerouted
        verify(transportService).sendRequest(same(otherNode), eq(InferenceAction.NAME), any(), any());
        // Verify local execution didn't happen
        verify(listener, never()).onResponse(any());
        // Verify that request metric attributes were NOT recorded on the node rerouting the request to another node
        verify(inferenceStats.inferenceDuration(), never()).record(anyLong(), any());
    }

    public void testRerouting_ToLocalNode_WithoutGoingThroughTransportLayerAgain() {
        DiscoveryNode localNode = mock(DiscoveryNode.class);
        String localNodeId = "local-node";
        when(localNode.getId()).thenReturn(localNodeId);

        // The local node is the only one responsible for serviceId
        when(nodeClient.getLocalNodeId()).thenReturn(localNodeId);
        when(inferenceServiceRateLimitCalculator.isTaskTypeReroutingSupported(serviceId, taskType)).thenReturn(true);
        var assignment = new RateLimitAssignment(List.of(localNode));
        when(inferenceServiceRateLimitCalculator.getRateLimitAssignment(serviceId, taskType)).thenReturn(assignment);

        mockService(listener -> listener.onResponse(mock()));
        var listener = doExecute(taskType);

        verify(listener).onResponse(any());
        // Verify request was handled locally (not rerouted using TransportService)
        verify(transportService, never()).sendRequest(any(), any(), any(), any());
        // Verify request metric attributes were recorded on the node performing inference
        verify(inferenceStats.inferenceDuration()).record(anyLong(), assertArg(attributes -> {
            assertThat(attributes.get("rerouted"), is(Boolean.FALSE));
            assertThat(attributes.get("node_id"), is(localNodeId));
        }));
    }

    public void testRerouting_HandlesTransportException_FromOtherNode() {
        DiscoveryNode otherNode = mock(DiscoveryNode.class);
        when(otherNode.getId()).thenReturn("other-node");

        when(nodeClient.getLocalNodeId()).thenReturn("local-node");
        when(inferenceServiceRateLimitCalculator.isTaskTypeReroutingSupported(serviceId, taskType)).thenReturn(true);
        var assignment = new RateLimitAssignment(List.of(otherNode));
        when(inferenceServiceRateLimitCalculator.getRateLimitAssignment(serviceId, taskType)).thenReturn(assignment);

        mockService(listener -> listener.onResponse(mock()));

        TransportException expectedException = new TransportException("Failed to route");
        doAnswer(invocation -> {
            TransportResponseHandler<?> handler = invocation.getArgument(3);
            handler.handleException(expectedException);
            return null;
        }).when(transportService).sendRequest(any(), any(), any(), any());

        var listener = doExecute(taskType);

        // Verify request was rerouted
        verify(transportService).sendRequest(same(otherNode), eq(InferenceAction.NAME), any(), any());
        // Verify local execution didn't happen
        verify(listener, never()).onResponse(any());
        // Verify exception was propagated from "other-node" to "local-node"
        verify(listener).onFailure(same(expectedException));
        // Verify that request metric attributes were NOT recorded on the node rerouting the request to another node
        verify(inferenceStats.inferenceDuration(), never()).record(anyLong(), any());
    }
}
