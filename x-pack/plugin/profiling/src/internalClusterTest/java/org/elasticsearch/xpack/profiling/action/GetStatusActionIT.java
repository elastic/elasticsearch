/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.action;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class GetStatusActionIT extends ProfilingTestCase {
    @Override
    protected boolean requiresDataSetup() {
        // We need explicit control whether index template management is enabled, and thus we skip data setup.
        return false;
    }

    @Before
    public void setupCluster() {
        // dedicated master with a data node
        internalCluster().setBootstrapMasterNodeIndex(0);
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
    }

    public void testTimeoutIfResourcesNotCreated() throws Exception {
        // ECS enabled but templates haven't been installed yet; use a minimal timeout so it fires before
        // the master can install them.
        updateProfilingTemplatesEnabled(true);
        GetStatusAction.Request request = new GetStatusAction.Request(TEST_REQUEST_TIMEOUT, true, TimeValue.timeValueMillis(1));

        GetStatusAction.Response response = client().execute(GetStatusAction.INSTANCE, request).get();
        assertEquals(RestStatus.REQUEST_TIMEOUT, response.status());
        assertFalse(response.hasData());
    }

    public void testNoTimeoutIfNotWaiting() throws Exception {
        updateProfilingTemplatesEnabled(false);
        GetStatusAction.Request request = new GetStatusAction.Request(TEST_REQUEST_TIMEOUT, false, randomTimeValue());

        GetStatusAction.Response response = client().execute(GetStatusAction.INSTANCE, request).get();
        assertEquals(RestStatus.OK, response.status());
        // ECS templates are disabled; no ECS resources have been created
        assertFalse(response.isEcsResourcesCreated());
        assertFalse(response.hasData());
    }

    public void testWaitsUntilResourcesAreCreated() throws Exception {
        updateProfilingTemplatesEnabled(true);
        GetStatusAction.Request request = new GetStatusAction.Request(
            TEST_REQUEST_TIMEOUT,
            true,
            // higher timeout since we have more shards than usual:
            TimeValue.timeValueSeconds(120)
        );

        GetStatusAction.Response response = client().execute(GetStatusAction.INSTANCE, request).get();
        assertEquals(RestStatus.OK, response.status());
        assertTrue(response.isEcsResourcesCreated());
        assertFalse(response.hasData());
    }

    public void testHasData() throws Exception {
        doSetupData();
        GetStatusAction.Request request = new GetStatusAction.Request(TEST_REQUEST_TIMEOUT, true, TEST_REQUEST_TIMEOUT);
        GetStatusAction.Response response = client().execute(GetStatusAction.INSTANCE, request).get();
        assertEquals(RestStatus.OK, response.status());
        assertTrue(response.isEcsResourcesCreated());
        assertTrue(response.hasData());
    }
}
