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
        updateProfilingTemplatesEnabled(false);
        GetStatusAction.Request request = new GetStatusAction.Request(
            TEST_REQUEST_TIMEOUT,
            true,
            // shorter than the default timeout to avoid excessively long execution:
            TimeValue.timeValueSeconds(15)
        );

        GetStatusAction.Response response = client().execute(GetStatusAction.INSTANCE, request).get();
        assertEquals(RestStatus.REQUEST_TIMEOUT, response.status());
        assertFalse(response.isResourcesCreated());
        assertFalse(response.hasData());
    }

    public void testNoTimeoutIfNotWaiting() throws Exception {
        updateProfilingTemplatesEnabled(false);
        GetStatusAction.Request request = new GetStatusAction.Request(TEST_REQUEST_TIMEOUT, false, randomTimeValue());

        GetStatusAction.Response response = client().execute(GetStatusAction.INSTANCE, request).get();
        assertEquals(RestStatus.OK, response.status());
        assertFalse(response.isResourcesCreated());
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
        assertTrue(response.isResourcesCreated());
        assertFalse(response.hasData());
    }

    public void testHasData() throws Exception {
        doSetupData();
        GetStatusAction.Request request = new GetStatusAction.Request(TEST_REQUEST_TIMEOUT, true, TEST_REQUEST_TIMEOUT);
        GetStatusAction.Response response = client().execute(GetStatusAction.INSTANCE, request).get();
        assertEquals(RestStatus.OK, response.status());
        assertTrue(response.isResourcesCreated());
        assertTrue(response.hasData());
    }
}
