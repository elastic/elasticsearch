/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.action;

import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.xpack.autoscaling.AutoscalingIntegTestCase;
import org.elasticsearch.xpack.autoscaling.policy.AutoscalingPolicy;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.autoscaling.AutoscalingTestCase.randomAutoscalingPolicyOfName;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class TransportGetAutoscalingPolicyActionIT extends AutoscalingIntegTestCase {

    public void testGetPolicy() {
        final String name = randomAlphaOfLength(8);
        final AutoscalingPolicy expectedPolicy = randomAutoscalingPolicyOfName(name);
        final PutAutoscalingPolicyAction.Request putRequest = new PutAutoscalingPolicyAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            expectedPolicy.name(),
            expectedPolicy.roles(),
            expectedPolicy.deciders()
        );
        assertAcked(client().execute(PutAutoscalingPolicyAction.INSTANCE, putRequest).actionGet());
        // we trust that the policy is in the cluster state since we have tests for putting policies
        final GetAutoscalingPolicyAction.Request getRequest = new GetAutoscalingPolicyAction.Request(TEST_REQUEST_TIMEOUT, name);
        final AutoscalingPolicy actualPolicy = client().execute(GetAutoscalingPolicyAction.INSTANCE, getRequest).actionGet().policy();
        assertThat(expectedPolicy, equalTo(actualPolicy));
    }

    public void testGetNonExistentPolicy() {
        final String name = randomAlphaOfLength(8);
        final GetAutoscalingPolicyAction.Request getRequest = new GetAutoscalingPolicyAction.Request(TEST_REQUEST_TIMEOUT, name);
        final ResourceNotFoundException e = expectThrows(
            ResourceNotFoundException.class,
            client().execute(GetAutoscalingPolicyAction.INSTANCE, getRequest)
        );
        assertThat(e.getMessage(), containsString("autoscaling policy with name [" + name + "] does not exist"));
    }

}
