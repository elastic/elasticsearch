/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ReservedStateHandlerMetadata;
import org.elasticsearch.cluster.metadata.ReservedStateMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.reservedstate.action.ReservedClusterSettingsAction;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class ReservedStateAwareHandledTransportActionTests extends ESTestCase {
    public void testRejectImmutableConflictClusterStateUpdate() {
        ReservedStateHandlerMetadata hmOne = new ReservedStateHandlerMetadata(ReservedClusterSettingsAction.NAME, Set.of("a", "b"));
        ReservedStateHandlerMetadata hmThree = new ReservedStateHandlerMetadata(ReservedClusterSettingsAction.NAME, Set.of("e", "f"));
        ReservedStateMetadata omOne = ReservedStateMetadata.builder("namespace_one").putHandler(hmOne).build();
        ReservedStateMetadata omTwo = ReservedStateMetadata.builder("namespace_two").putHandler(hmThree).build();

        Metadata metadata = Metadata.builder().put(omOne).put(omTwo).build();

        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).metadata(metadata).build();
        ClusterService clusterService = mock(ClusterService.class);
        doReturn(clusterState).when(clusterService).state();

        Action handler = new Action("internal:testAction", clusterService, mock(TransportService.class), mock(ActionFilters.class));

        // nothing should happen here, since the request doesn't touch any of the immutable state keys
        var future = new PlainActionFuture<FakeResponse>();
        handler.doExecute(mock(Task.class), new DummyRequest(), future);
        assertNotNull(future.actionGet());

        ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest().persistentSettings(
            Settings.builder().put("a", "a value").build()
        ).transientSettings(Settings.builder().put("e", "e value").build());

        FakeReservedStateAwareAction action = new FakeReservedStateAwareAction(
            "internal:testClusterSettings",
            clusterService,
            mock(TransportService.class),
            mock(ActionFilters.class),
            null
        );

        assertTrue(expectThrows(IllegalArgumentException.class, () -> action.doExecute(mock(Task.class), request, new ActionListener<>() {
            @Override
            public void onResponse(FakeResponse fakeResponse) {
                fail("Shouldn't reach here");
            }

            @Override
            public void onFailure(Exception e) {
                assertNotNull(e);
            }
        })).getMessage().contains("with errors: [[a] set as read-only by [namespace_one], " + "[e] set as read-only by [namespace_two]"));

        ClusterUpdateSettingsRequest okRequest = new ClusterUpdateSettingsRequest().persistentSettings(
            Settings.builder().put("m", "m value").build()
        ).transientSettings(Settings.builder().put("n", "n value").build());

        // this should just work, no conflicts
        action.doExecute(mock(Task.class), okRequest, new ActionListener<>() {
            @Override
            public void onResponse(FakeResponse fakeResponse) {
                assertNotNull(fakeResponse);
            }

            @Override
            public void onFailure(Exception e) {
                fail("Shouldn't reach here");
            }
        });
    }

    static class Action extends ReservedStateAwareHandledTransportAction<DummyRequest, FakeResponse> {
        protected Action(String actionName, ClusterService clusterService, TransportService transportService, ActionFilters actionFilters) {
            super(actionName, clusterService, transportService, actionFilters, null);
        }

        @Override
        public Optional<String> reservedStateHandlerName() {
            return Optional.of("test_reserved_state_action");
        }

        @Override
        protected void doExecuteProtected(Task task, DummyRequest request, ActionListener<FakeResponse> listener) {
            listener.onResponse(new FakeResponse());
        }
    }

    static class DummyRequest extends ActionRequest {
        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    static class FakeResponse extends ActionResponse {
        @Override
        public void writeTo(StreamOutput out) throws IOException {}
    }

    static class FakeReservedStateAwareAction extends ReservedStateAwareHandledTransportAction<ClusterUpdateSettingsRequest, FakeResponse> {
        protected FakeReservedStateAwareAction(
            String actionName,
            ClusterService clusterService,
            TransportService transportService,
            ActionFilters actionFilters,
            Writeable.Reader<ClusterUpdateSettingsRequest> clusterUpdateSettingsRequestReader
        ) {
            super(actionName, clusterService, transportService, actionFilters, clusterUpdateSettingsRequestReader);
        }

        @Override
        protected void doExecuteProtected(Task task, ClusterUpdateSettingsRequest request, ActionListener<FakeResponse> listener) {
            listener.onResponse(new FakeResponse());
        }

        @Override
        public Optional<String> reservedStateHandlerName() {
            return Optional.of(ReservedClusterSettingsAction.NAME);
        }

        @Override
        public Set<String> modifiedKeys(ClusterUpdateSettingsRequest request) {
            Settings allSettings = Settings.builder().put(request.persistentSettings()).put(request.transientSettings()).build();
            return allSettings.keySet();
        }
    }
}
