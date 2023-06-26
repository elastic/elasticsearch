/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.reservedstate;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.reservedstate.NonStateTransformResult;
import org.elasticsearch.reservedstate.TransformState;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.security.action.rolemapping.ReservedRoleMappingAction;
import org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

/**
 * Tests that the ReservedRoleMappingAction does validation, can add and remove role mappings
 */
public class ReservedRoleMappingActionTests extends ESTestCase {
    private TransformState processJSON(ReservedRoleMappingAction action, TransformState prevState, String json) throws Exception {
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            var content = action.fromXContent(parser);
            var state = action.transform(content, prevState);

            CountDownLatch latch = new CountDownLatch(1);
            AtomicReference<Set<String>> updatedKeys = new AtomicReference<>();
            AtomicReference<Exception> error = new AtomicReference<>();
            state.nonStateTransform().accept(new ActionListener<>() {
                @Override
                public void onResponse(NonStateTransformResult nonStateTransformResult) {
                    updatedKeys.set(nonStateTransformResult.updatedKeys());
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    error.set(e);
                    latch.countDown();
                }
            });

            latch.await();
            if (error.get() != null) {
                throw error.get();
            }
            return new TransformState(state.state(), updatedKeys.get());
        }
    }

    public void testValidation() {
        var nativeRoleMappingStore = mockNativeRoleMappingStore();

        ClusterState state = ClusterState.builder(new ClusterName("elasticsearch")).build();
        TransformState prevState = new TransformState(state, Collections.emptySet());
        ReservedRoleMappingAction action = new ReservedRoleMappingAction(nativeRoleMappingStore);
        action.securityIndexRecovered();

        String badPolicyJSON = """
            {
               "everyone_kibana": {
                  "enabled": true,
                  "roles": [ "inter_planetary_role" ],
                  "rules": { "field": { "username": "*" } },
                  "metadata": {
                     "uuid" : "b9a59ba9-6b92-4be2-bb8d-02bb270cb3a7",
                     "_reserved": true
                  }
               },
               "everyone_fleet": {
                  "enabled": true,
                  "roles": [ "fleet_user" ],
                  "metadata": {
                     "uuid" : "b9a59ba9-6b92-4be3-bb8d-02bb270cb3a7"
                  }
               }
            }""";

        assertEquals(
            "failed to parse role-mapping [everyone_fleet]. missing field [rules]",
            expectThrows(ParsingException.class, () -> processJSON(action, prevState, badPolicyJSON)).getMessage()
        );
    }

    public void testAddRemoveRoleMapping() throws Exception {
        var nativeRoleMappingStore = mockNativeRoleMappingStore();

        ClusterState state = ClusterState.builder(new ClusterName("elasticsearch")).build();
        TransformState prevState = new TransformState(state, Collections.emptySet());
        ReservedRoleMappingAction action = new ReservedRoleMappingAction(nativeRoleMappingStore);
        action.securityIndexRecovered();

        String emptyJSON = "";

        TransformState updatedState = processJSON(action, prevState, emptyJSON);
        assertEquals(0, updatedState.keys().size());
        assertEquals(prevState.state(), updatedState.state());

        String json = """
            {
               "everyone_kibana": {
                  "enabled": true,
                  "roles": [ "kibana_user" ],
                  "rules": { "field": { "username": "*" } },
                  "metadata": {
                     "uuid" : "b9a59ba9-6b92-4be2-bb8d-02bb270cb3a7",
                     "_reserved": true
                  }
               },
               "everyone_fleet": {
                  "enabled": true,
                  "roles": [ "fleet_user" ],
                  "rules": { "field": { "username": "*" } },
                  "metadata": {
                     "uuid" : "b9a59ba9-6b92-4be3-bb8d-02bb270cb3a7",
                     "_reserved": true
                  }
               }
            }""";

        prevState = updatedState;
        updatedState = processJSON(action, prevState, json);
        assertThat(updatedState.keys(), containsInAnyOrder("everyone_kibana", "everyone_fleet"));

        updatedState = processJSON(action, prevState, emptyJSON);
        assertThat(updatedState.keys(), empty());
    }

    @SuppressWarnings("unchecked")
    public void testNonStateTransformWaitsOnAsyncActions() throws Exception {
        var nativeRoleMappingStore = mockNativeRoleMappingStore();

        doAnswer(invocation -> {
            new Thread(() -> {
                // Simulate put role mapping async action taking a while
                try {
                    Thread.sleep(1_000);
                    ((ActionListener<Boolean>) invocation.getArgument(1)).onFailure(new IllegalStateException("err_done"));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }).start();

            return null;
        }).when(nativeRoleMappingStore).putRoleMapping(any(), any());

        doAnswer(invocation -> {
            new Thread(() -> {
                // Simulate delete role mapping async action taking a while
                try {
                    Thread.sleep(1_000);
                    ((ActionListener<Boolean>) invocation.getArgument(1)).onFailure(new IllegalStateException("err_done"));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }).start();

            return null;
        }).when(nativeRoleMappingStore).deleteRoleMapping(any(), any());

        ClusterState state = ClusterState.builder(new ClusterName("elasticsearch")).build();
        TransformState updatedState = new TransformState(state, Collections.emptySet());
        ReservedRoleMappingAction action = new ReservedRoleMappingAction(nativeRoleMappingStore);
        action.securityIndexRecovered();

        String json = """
            {
               "everyone_kibana": {
                  "enabled": true,
                  "roles": [ "kibana_user" ],
                  "rules": { "field": { "username": "*" } },
                  "metadata": {
                     "uuid" : "b9a59ba9-6b92-4be2-bb8d-02bb270cb3a7",
                     "_reserved": true
                  }
               },
               "everyone_fleet": {
                  "enabled": true,
                  "roles": [ "fleet_user" ],
                  "rules": { "field": { "username": "*" } },
                  "metadata": {
                     "uuid" : "a9a59ba9-6b92-4be2-bb8d-02bb270cb3a7",
                     "_reserved": true
                  }
               }
            }""";

        assertEquals(
            "err_done",
            expectThrows(IllegalStateException.class, () -> processJSON(action, new TransformState(state, Collections.emptySet()), json))
                .getMessage()
        );

        // Now that we've tested that we wait on putRoleMapping correctly, let it finish without exception, so we can test error on delete
        doAnswer(invocation -> {
            ((ActionListener<Boolean>) invocation.getArgument(1)).onResponse(true);
            return null;
        }).when(nativeRoleMappingStore).putRoleMapping(any(), any());

        updatedState = processJSON(action, updatedState, json);
        assertThat(updatedState.keys(), containsInAnyOrder("everyone_kibana", "everyone_fleet"));

        final TransformState currentState = new TransformState(updatedState.state(), updatedState.keys());

        assertEquals("err_done", expectThrows(IllegalStateException.class, () -> processJSON(action, currentState, "")).getMessage());
    }

    @SuppressWarnings("unchecked")
    private NativeRoleMappingStore mockNativeRoleMappingStore() {
        final NativeRoleMappingStore nativeRoleMappingStore = spy(
            new NativeRoleMappingStore(Settings.EMPTY, mock(Client.class), mock(SecurityIndexManager.class), mock(ScriptService.class))
        );

        doAnswer(invocation -> {
            ((ActionListener<Boolean>) invocation.getArgument(1)).onResponse(true);
            return null;
        }).when(nativeRoleMappingStore).putRoleMapping(any(), any());

        doAnswer(invocation -> {
            ((ActionListener<Boolean>) invocation.getArgument(1)).onResponse(true);
            return null;
        }).when(nativeRoleMappingStore).deleteRoleMapping(any(), any());

        return nativeRoleMappingStore;
    }
}
