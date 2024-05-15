/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.test.rest.ESRestTestCase;

public class HealthReportIT extends ESRestTestCase {

    /**
     * This test is a Java REST test instead of a YAML one, as the health node might need some time before it has received the health info
     * from all the nodes. Until it has received all health info, the status will be "unknown", so we'll wait until the status turns green.
     */
    public void testGetHealth() throws Exception {
        Request request = new Request("GET", "_health_report");
        Response response = client().performRequest(request);
        assertBusy(() -> {
            assertEquals(200, response.getStatusLine().getStatusCode());
            var mapView = XContentTestUtils.createJsonMapView(response.getEntity().getContent());
            assertEquals("green", mapView.get("status"));
            assertEquals("green", mapView.get("indicators.master_is_stable.status"));
            assertEquals("The cluster has a stable master node", mapView.get("indicators.master_is_stable.symptom"));
        });
    }
}
