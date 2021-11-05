/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.action;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.autoscaling.AutoscalingIntegTestCase;
import org.elasticsearch.xpack.autoscaling.AutoscalingMetadata;
import org.elasticsearch.xpack.autoscaling.policy.AutoscalingPolicy;

import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.autoscaling.AutoscalingTestCase.mutateAutoscalingDeciders;
import static org.elasticsearch.xpack.autoscaling.AutoscalingTestCase.randomAutoscalingDeciders;
import static org.elasticsearch.xpack.autoscaling.AutoscalingTestCase.randomAutoscalingPolicy;
import static org.elasticsearch.xpack.autoscaling.AutoscalingTestCase.randomRoles;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.sameInstance;

public class TransportPutAutoscalingPolicyActionIT extends AutoscalingIntegTestCase {

    public void testAddPolicy() {
        final AutoscalingPolicy policy = putRandomAutoscalingPolicy();
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final AutoscalingMetadata metadata = state.metadata().custom(AutoscalingMetadata.NAME);
        assertNotNull(metadata);
        assertThat(metadata.policies(), hasKey(policy.name()));
        assertThat(metadata.policies().get(policy.name()).policy(), equalTo(policy));
    }

    public void testUpdatePolicy() {
        final AutoscalingPolicy policy = putRandomAutoscalingPolicy();
        final AutoscalingPolicy updatedPolicy = new AutoscalingPolicy(
            policy.name(),
            new TreeSet<>(randomSubsetOf(randomIntBetween(1, 5), List.of("data", "data_content", "data_hot", "data_warm", "data_cold"))),
            mutateAutoscalingDeciders(policy.deciders())
        );
        putAutoscalingPolicy(updatedPolicy);
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final AutoscalingMetadata metadata = state.metadata().custom(AutoscalingMetadata.NAME);
        assertNotNull(metadata);
        assertThat(metadata.policies(), hasKey(policy.name()));
        assertThat(metadata.policies().get(policy.name()).policy(), equalTo(updatedPolicy));
    }

    public void testNoOpPolicy() {
        final AutoscalingPolicy policy = putRandomAutoscalingPolicy();
        final ClusterState beforeState = internalCluster().getInstance(ClusterService.class, internalCluster().getMasterName()).state();
        putAutoscalingPolicy(policy);
        final ClusterState afterState = internalCluster().getInstance(ClusterService.class, internalCluster().getMasterName()).state();
        assertThat(
            beforeState.metadata().custom(AutoscalingMetadata.NAME),
            sameInstance(afterState.metadata().custom(AutoscalingMetadata.NAME))
        );
    }

    public void testPutPolicyIllegalName() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> putAutoscalingPolicy(new AutoscalingPolicy(randomAlphaOfLength(8) + "*", randomRoles(), randomAutoscalingDeciders()))
        );

        assertThat(
            exception.getMessage(),
            containsString("name must not contain the following characters " + Strings.INVALID_FILENAME_CHARS)
        );
    }

    public void testPutNoDeciderPolicy() {
        String policyName = randomAlphaOfLength(8);
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> putAutoscalingPolicy(new AutoscalingPolicy(policyName, new TreeSet<>(), new TreeMap<>()))
        );

        assertThat(
            exception.getMessage(),
            containsString("no default nor user configured deciders for policy [" + policyName + "] with roles [[]]")
        );
    }

    private AutoscalingPolicy putRandomAutoscalingPolicy() {
        final AutoscalingPolicy policy = randomAutoscalingPolicy();
        putAutoscalingPolicy(policy);
        return policy;
    }

    private void putAutoscalingPolicy(final AutoscalingPolicy policy) {
        final PutAutoscalingPolicyAction.Request request = new PutAutoscalingPolicyAction.Request(
            policy.name(),
            policy.roles(),
            policy.deciders()
        );
        assertAcked(client().execute(PutAutoscalingPolicyAction.INSTANCE, request).actionGet());
    }

}
