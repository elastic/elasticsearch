/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.junit.Before;
import org.mockito.Mockito;
import org.mockito.stubbing.Stubber;

import static org.elasticsearch.xpack.core.ilm.LifecycleSettings.LIFECYCLE_STEP_MASTER_TIMEOUT;
import static org.hamcrest.Matchers.equalTo;

public abstract class AbstractStepMasterTimeoutTestCase<T extends AsyncActionStep> extends AbstractStepTestCase<T> {

    protected Client client;
    protected AdminClient adminClient;
    protected IndicesAdminClient indicesClient;

    @Before
    public void setup() {
        client = Mockito.mock(Client.class);
        adminClient = Mockito.mock(AdminClient.class);
        Mockito.when(client.admin()).thenReturn(adminClient);
        indicesClient = Mockito.mock(IndicesAdminClient.class);
        Mockito.when(adminClient.indices()).thenReturn(indicesClient);
    }

    public void testMasterTimeout() {
        checkMasterTimeout(TimeValue.timeValueSeconds(30),
            ClusterState.builder(ClusterName.DEFAULT).metaData(MetaData.builder().build()).build());
        checkMasterTimeout(TimeValue.timeValueSeconds(10),
            ClusterState.builder(ClusterName.DEFAULT)
                .metaData(MetaData.builder()
                    .persistentSettings(Settings.builder().put(LIFECYCLE_STEP_MASTER_TIMEOUT, "10s").build())
                    .build())
                .build());
    }

    @SuppressWarnings("rawtypes")
    private void checkMasterTimeout(TimeValue timeValue, ClusterState currentClusterState) {
        IndexMetaData indexMetadata = getIndexMetaData();

        Stubber checkTimeout = Mockito.doAnswer(invocation -> {
            for(Object argument: invocation.getArguments()){
                if(argument instanceof MasterNodeRequest)
                {
                    @SuppressWarnings("rawtypes") MasterNodeRequest<?> request = (MasterNodeRequest) argument;
                    assertThat(request.masterNodeTimeout(), equalTo(timeValue));
                }
            }
            return null;
        });
        mockRequestCall(checkTimeout);
        T step = createRandomInstance();
        step.performAction(indexMetadata, currentClusterState, null, new AsyncActionStep.Listener() {
            @Override
            public void onResponse(boolean complete) {

            }

            @Override
            public void onFailure(Exception e) {

            }
        });
    }

    protected abstract IndexMetaData getIndexMetaData();

    protected abstract void mockRequestCall(Stubber checkTimeout);
}
