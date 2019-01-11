/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.xpack.core.indexlifecycle.Step.StepKey;
import org.mockito.Mockito;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsNull.notNullValue;

public class WaitForYellowStepTests extends AbstractStepTestCase<WaitForYellowStep> {

    @Override
    protected WaitForYellowStep createRandomInstance() {
        StepKey stepKey = randomStepKey();
        StepKey nextStepKey = randomStepKey();
        return new WaitForYellowStep(stepKey, nextStepKey, Mockito.mock(Client.class));
    }

    @Override
    protected WaitForYellowStep mutateInstance(WaitForYellowStep instance) {
        StepKey key = instance.getKey();
        StepKey nextKey = instance.getNextStepKey();

        if (randomBoolean()) {
            key = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
        } else {
            nextKey = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
        }

        return new WaitForYellowStep(key, nextKey, instance.getClient());
    }

    @Override
    protected WaitForYellowStep copyInstance(WaitForYellowStep instance) {
        return new WaitForYellowStep(instance.getKey(), instance.getNextStepKey(), instance.getClient());
    }

    public void testConditionMet() {
        IndexMetaData indexMetadata = IndexMetaData.builder("former-follower-index")
            .settings(settings(Version.CURRENT))
            .numberOfShards(2)
            .numberOfReplicas(0)
            .build();
        Client client = Mockito.mock(Client.class);
        String indexName = indexMetadata.getIndex().getName();
        ClusterHealthResponse healthResponse = new ClusterHealthResponse("_cluster", new String[]{indexName}, ClusterState.EMPTY_STATE);
        mockClusterHealthCall(client, indexMetadata.getIndex().getName(), healthResponse);

        WaitForYellowStep step = new WaitForYellowStep(randomStepKey(), randomStepKey(), client);
        final boolean[] conditionMetHolder = new boolean[1];
        final ToXContentObject[] informationContextHolder = new ToXContentObject[1];
        final Exception[] exceptionHolder = new Exception[1];
        step.evaluateCondition(indexMetadata, new AsyncWaitStep.Listener() {
            @Override
            public void onResponse(boolean conditionMet, ToXContentObject informationContext) {
                conditionMetHolder[0] = conditionMet;
                informationContextHolder[0] = informationContext;
            }

            @Override
            public void onFailure(Exception e) {
                exceptionHolder[0] = e;
            }
        });

        assertThat(conditionMetHolder[0], is(true));
        assertThat(informationContextHolder[0], nullValue());
        assertThat(exceptionHolder[0], nullValue());
    }

    public void testConditionNotMet() {
        IndexMetaData indexMetadata = IndexMetaData.builder("former-follower-index")
            .settings(settings(Version.CURRENT))
            .numberOfShards(2)
            .numberOfReplicas(0)
            .build();
        Client client = Mockito.mock(Client.class);
        String indexName = indexMetadata.getIndex().getName();
        ClusterHealthResponse healthResponse = new ClusterHealthResponse("_cluster", new String[]{indexName}, ClusterState.EMPTY_STATE);
        healthResponse.setTimedOut(true);
        mockClusterHealthCall(client, indexMetadata.getIndex().getName(), healthResponse);

        WaitForYellowStep step = new WaitForYellowStep(randomStepKey(), randomStepKey(), client);
        final boolean[] conditionMetHolder = new boolean[1];
        final ToXContentObject[] informationContextHolder = new ToXContentObject[1];
        final Exception[] exceptionHolder = new Exception[1];
        step.evaluateCondition(indexMetadata, new AsyncWaitStep.Listener() {
            @Override
            public void onResponse(boolean conditionMet, ToXContentObject informationContext) {
                conditionMetHolder[0] = conditionMet;
                informationContextHolder[0] = informationContext;
            }

            @Override
            public void onFailure(Exception e) {
                exceptionHolder[0] = e;
            }
        });

        assertThat(conditionMetHolder[0], is(false));
        assertThat(informationContextHolder[0], notNullValue());
        assertThat(exceptionHolder[0], nullValue());
        WaitForYellowStep.Info info = (WaitForYellowStep.Info) informationContextHolder[0];
        assertThat(info.getMessage(), equalTo("cluster health request timed out waiting for yellow status"));
    }

    private void mockClusterHealthCall(Client client, String expectedIndexName, ClusterHealthResponse healthResponse) {
        AdminClient adminClient = Mockito.mock(AdminClient.class);
        ClusterAdminClient clusterAdminClient = Mockito.mock(ClusterAdminClient.class);
        Mockito.when(client.admin()).thenReturn(adminClient);
        Mockito.when(adminClient.cluster()).thenReturn(clusterAdminClient);
        Mockito.doAnswer(invocationOnMock -> {
            ClusterHealthRequest request = (ClusterHealthRequest) invocationOnMock.getArguments()[0];
            assertThat(request.indices().length, equalTo(1));
            assertThat(request.indices()[0], equalTo(expectedIndexName));
            assertThat(request.waitForStatus(), equalTo(ClusterHealthStatus.YELLOW));

            @SuppressWarnings("unchecked")
            ActionListener<ClusterHealthResponse> listener = (ActionListener<ClusterHealthResponse>) invocationOnMock.getArguments()[1];
            listener.onResponse(healthResponse);
            return null;
        }).when(clusterAdminClient).health(Mockito.any(), Mockito.any());
    }
}
