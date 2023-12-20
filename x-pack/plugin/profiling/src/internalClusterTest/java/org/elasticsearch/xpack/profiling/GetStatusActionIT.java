/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;

public class GetStatusActionIT extends ProfilingTestCase {
    @Override
    protected boolean requiresDataSetup() {
        // We need explicit control whether index template management is enabled, and thus we skip data setup.
        return false;
    }

    public void testTimeoutIfResourcesNotCreated() throws Exception {
        updateProfilingTemplatesEnabled(false);
        GetStatusAction.Request request = new GetStatusAction.Request();
        request.waitForResourcesCreated(true);
        // shorter than the default timeout to avoid excessively long execution
        request.timeout(TimeValue.timeValueSeconds(15));

        GetStatusAction.Response response = client().execute(GetStatusAction.INSTANCE, request).get();
        assertEquals(RestStatus.REQUEST_TIMEOUT, response.status());
        assertFalse(response.isResourcesCreated());
    }

    public void testNoTimeoutIfNotWaiting() throws Exception {
        updateProfilingTemplatesEnabled(false);
        GetStatusAction.Request request = new GetStatusAction.Request();
        request.waitForResourcesCreated(false);

        GetStatusAction.Response response = client().execute(GetStatusAction.INSTANCE, request).get();
        assertEquals(RestStatus.OK, response.status());
        assertFalse(response.isResourcesCreated());
    }

    public void testWaitsUntilResourcesAreCreated() throws Exception {
        updateProfilingTemplatesEnabled(true);
        GetStatusAction.Request request = new GetStatusAction.Request();
        request.waitForResourcesCreated(true);

        GetStatusAction.Response response = client().execute(GetStatusAction.INSTANCE, request).get();
        assertEquals(RestStatus.OK, response.status());
        assertTrue(response.isResourcesCreated());
    }
}
