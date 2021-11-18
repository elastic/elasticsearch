/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.autoscaling.action.DeleteAutoscalingPolicyAction;
import org.elasticsearch.xpack.autoscaling.action.GetAutoscalingPolicyAction;
import org.elasticsearch.xpack.autoscaling.action.PutAutoscalingPolicyAction;
import org.elasticsearch.xpack.autoscaling.policy.AutoscalingPolicy;
import org.junit.Before;

import java.nio.file.Path;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.autoscaling.AutoscalingTestCase.randomAutoscalingPolicy;
import static org.elasticsearch.xpack.autoscaling.AutoscalingTestCase.randomAutoscalingPolicyOfName;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class AutoscalingSnapshotsIT extends AutoscalingIntegTestCase {

    public static final String REPO = "repo";
    public static final String SNAPSHOT = "snap";

    @Before
    public void setup() throws Exception {
        Path location = randomRepoPath();
        logger.info("--> creating repository [{}] [{}]", REPO, "fs");
        assertAcked(clusterAdmin().preparePutRepository(REPO).setType("fs").setSettings(Settings.builder().put("location", location)));
    }

    public void testAutoscalingPolicyWillNotBeRestored() {
        final Client client = client();

        AutoscalingPolicy policy = randomAutoscalingPolicy();
        putPolicy(policy);

        CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot(REPO, SNAPSHOT)
            .setWaitForCompletion(true)
            .setIncludeGlobalState(true)
            .get();
        assertEquals(RestStatus.OK, createSnapshotResponse.status());

        final boolean deletePolicy = randomBoolean();
        if (deletePolicy) {
            final DeleteAutoscalingPolicyAction.Request deleteRequest = new DeleteAutoscalingPolicyAction.Request(policy.name());
            assertAcked(client.execute(DeleteAutoscalingPolicyAction.INSTANCE, deleteRequest).actionGet());
        } else {
            // Update the policy
            policy = randomAutoscalingPolicyOfName(policy.name());
            putPolicy(policy);
        }
        final AutoscalingPolicy anotherPolicy = randomAutoscalingPolicy();
        putPolicy(anotherPolicy);

        RestoreSnapshotResponse restoreSnapshotResponse = client.admin()
            .cluster()
            .prepareRestoreSnapshot(REPO, SNAPSHOT)
            .setWaitForCompletion(true)
            .setRestoreGlobalState(true)
            .get();
        assertEquals(RestStatus.OK, restoreSnapshotResponse.status());

        if (deletePolicy) {
            assertPolicyNotFound(policy);
        } else {
            assertPolicy(policy);
        }
        assertPolicy(anotherPolicy);
    }

    private void putPolicy(AutoscalingPolicy policy) {
        final PutAutoscalingPolicyAction.Request request = new PutAutoscalingPolicyAction.Request(
            policy.name(),
            policy.roles(),
            policy.deciders()
        );
        assertAcked(client().execute(PutAutoscalingPolicyAction.INSTANCE, request).actionGet());
    }

    private void assertPolicy(AutoscalingPolicy policy) {
        final GetAutoscalingPolicyAction.Request getRequest = new GetAutoscalingPolicyAction.Request(policy.name());
        final AutoscalingPolicy actualPolicy = client().execute(GetAutoscalingPolicyAction.INSTANCE, getRequest).actionGet().policy();
        assertThat(actualPolicy, equalTo(policy));
    }

    private void assertPolicyNotFound(AutoscalingPolicy policy) {
        final GetAutoscalingPolicyAction.Request getRequest = new GetAutoscalingPolicyAction.Request(policy.name());
        final ResourceNotFoundException e = expectThrows(
            ResourceNotFoundException.class,
            () -> client().execute(GetAutoscalingPolicyAction.INSTANCE, getRequest).actionGet().policy()
        );
        assertThat(e.getMessage(), containsString("autoscaling policy with name [" + policy.name() + "] does not exist"));
    }
}
