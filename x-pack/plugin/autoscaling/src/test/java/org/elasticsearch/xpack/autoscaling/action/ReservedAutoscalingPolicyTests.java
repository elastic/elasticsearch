/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.action;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.reservedstate.TransformState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.autoscaling.Autoscaling;
import org.elasticsearch.xpack.autoscaling.AutoscalingMetadata;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingCalculateCapacityService;
import org.elasticsearch.xpack.autoscaling.capacity.FixedAutoscalingDeciderService;

import java.util.Collections;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/**
 * Tests that the ReservedAutoscalingPolicyAction does validation, can add and remove autoscaling policies
 */
public class ReservedAutoscalingPolicyTests extends ESTestCase {
    private TransformState processJSON(ReservedAutoscalingPolicyAction action, TransformState prevState, String json) throws Exception {
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            return action.transform(action.fromXContent(parser), prevState);
        }
    }

    public void testValidation() {
        var mocks = createMockServices();

        ClusterState state = ClusterState.builder(new ClusterName("elasticsearch")).build();
        TransformState prevState = new TransformState(state, Collections.emptySet());
        ReservedAutoscalingPolicyAction action = new ReservedAutoscalingPolicyAction(mocks);

        String badPolicyJSON = """
            {
               "my_autoscaling_policy": {
                 "roles" : [ "data_hot" ],
                 "deciders": {
                   "fixed": {
                   }
                 }
               },
               "my_autoscaling_policy_1": {
                 "roles" : [ "data_hot" ],
                 "deciders": {
                   "random": {
                   }
                 }
               }
            }""";

        assertEquals(
            "unknown decider [random]",
            expectThrows(IllegalArgumentException.class, () -> processJSON(action, prevState, badPolicyJSON)).getMessage()
        );
    }

    public void testAddRemoveRoleMapping() throws Exception {
        var mocks = createMockServices();

        ClusterState state = ClusterState.builder(new ClusterName("elasticsearch")).build();
        TransformState prevState = new TransformState(state, Collections.emptySet());
        ReservedAutoscalingPolicyAction action = new ReservedAutoscalingPolicyAction(mocks);

        String emptyJSON = "";

        TransformState updatedState = processJSON(action, prevState, emptyJSON);
        assertEquals(0, updatedState.keys().size());
        assertEquals(prevState.state(), updatedState.state());

        String json = """
            {
               "my_autoscaling_policy": {
                 "roles" : [ "data_hot" ],
                 "deciders": {
                   "fixed": {
                   }
                 }
               },
               "my_autoscaling_policy_1": {
                 "roles" : [ "data_warm" ],
                 "deciders": {
                   "fixed": {
                   }
                 }
               }
            }""";

        prevState = updatedState;
        updatedState = processJSON(action, prevState, json);
        assertThat(updatedState.keys(), containsInAnyOrder("my_autoscaling_policy", "my_autoscaling_policy_1"));
        AutoscalingMetadata autoMetadata = updatedState.state().metadata().custom(AutoscalingMetadata.NAME);
        assertThat(autoMetadata.policies().keySet(), containsInAnyOrder("my_autoscaling_policy", "my_autoscaling_policy_1"));
        assertThat(autoMetadata.policies().get("my_autoscaling_policy").policy().roles(), containsInAnyOrder("data_hot"));
        assertThat(autoMetadata.policies().get("my_autoscaling_policy").policy().deciders().keySet(), containsInAnyOrder("fixed"));
        assertThat(autoMetadata.policies().get("my_autoscaling_policy_1").policy().roles(), containsInAnyOrder("data_warm"));
        assertThat(autoMetadata.policies().get("my_autoscaling_policy_1").policy().deciders().keySet(), containsInAnyOrder("fixed"));

        String halfJSON = """
            {
               "my_autoscaling_policy_1": {
                 "roles" : [ "data_warm" ],
                 "deciders": {
                   "fixed": {
                   }
                 }
               }
            }""";

        updatedState = processJSON(action, prevState, halfJSON);
        assertThat(updatedState.keys(), containsInAnyOrder("my_autoscaling_policy_1"));

        updatedState = processJSON(action, prevState, emptyJSON);
        assertThat(updatedState.keys(), empty());
    }

    private AutoscalingCalculateCapacityService.Holder createMockServices() {
        Autoscaling autoscaling = mock(Autoscaling.class);
        doReturn(Set.of(new FixedAutoscalingDeciderService())).when(autoscaling).createDeciderServices();

        return new AutoscalingCalculateCapacityService.Holder(autoscaling);
    }
}
