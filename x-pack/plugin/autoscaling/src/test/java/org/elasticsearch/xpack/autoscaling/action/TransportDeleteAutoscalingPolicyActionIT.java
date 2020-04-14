/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.xpack.autoscaling.AutoscalingIntegTestCase;
import org.elasticsearch.xpack.autoscaling.AutoscalingMetadata;
import org.elasticsearch.xpack.autoscaling.policy.AutoscalingPolicy;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.autoscaling.AutoscalingTestCase.randomAutoscalingPolicy;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

public class TransportDeleteAutoscalingPolicyActionIT extends AutoscalingIntegTestCase {

    public void testDeletePolicy() {
        final AutoscalingPolicy policy = randomAutoscalingPolicy();
        final PutAutoscalingPolicyAction.Request putRequest = new PutAutoscalingPolicyAction.Request(policy);
        assertAcked(client().execute(PutAutoscalingPolicyAction.INSTANCE, putRequest).actionGet());
        // we trust that the policy is in the cluster state since we have tests for putting policies
        final DeleteAutoscalingPolicyAction.Request deleteRequest = new DeleteAutoscalingPolicyAction.Request(policy.name());
        assertAcked(client().execute(DeleteAutoscalingPolicyAction.INSTANCE, deleteRequest).actionGet());
        // now verify that the policy is not in the cluster state
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final AutoscalingMetadata metadata = state.metadata().custom(AutoscalingMetadata.NAME);
        assertNotNull(metadata);
        assertThat(metadata.policies(), not(hasKey(policy.name())));
        // and verify that we can not obtain the policy via get
        final GetAutoscalingPolicyAction.Request getRequest = new GetAutoscalingPolicyAction.Request(policy.name());
        final ResourceNotFoundException e = expectThrows(
            ResourceNotFoundException.class,
            () -> client().execute(GetAutoscalingPolicyAction.INSTANCE, getRequest).actionGet()
        );
        assertThat(e.getMessage(), equalTo("autoscaling policy with name [" + policy.name() + "] does not exist"));
    }

    public void testDeleteNonExistentPolicy() {
        final String name = randomAlphaOfLength(8);
        final DeleteAutoscalingPolicyAction.Request deleteRequest = new DeleteAutoscalingPolicyAction.Request(name);
        final ResourceNotFoundException e = expectThrows(
            ResourceNotFoundException.class,
            () -> client().execute(DeleteAutoscalingPolicyAction.INSTANCE, deleteRequest).actionGet()
        );
        assertThat(e.getMessage(), containsString("autoscaling policy with name [" + name + "] does not exist"));
    }

}
