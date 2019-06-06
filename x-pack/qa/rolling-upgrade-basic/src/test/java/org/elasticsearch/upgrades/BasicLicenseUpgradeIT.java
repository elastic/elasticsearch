/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.upgrades;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;

import java.util.Map;

public class BasicLicenseUpgradeIT extends AbstractUpgradeTestCase {

    public void testOldAndMixedClusterHaveActiveBasic() throws Exception {
        assumeTrue("only runs against old or mixed cluster", clusterType == CLUSTER_TYPE.OLD || clusterType == CLUSTER_TYPE.MIXED);
        assertBusy(this::checkBasicLicense);
    }

    public void testNewClusterHasActiveNonExpiringBasic() throws Exception {
        assumeTrue("only runs against upgraded cluster", clusterType == CLUSTER_TYPE.UPGRADED);
        assertBusy(this::checkNonExpiringBasicLicense);
    }

    private void checkBasicLicense() throws Exception {
        Response licenseResponse = client().performRequest(new Request("GET", "/_license"));
        Map<String, Object> licenseResponseMap = entityAsMap(licenseResponse);
        Map<String, Object> licenseMap = (Map<String, Object>) licenseResponseMap.get("license");
        assertEquals("basic", licenseMap.get("type"));
        assertEquals("active", licenseMap.get("status"));
    }

    private void checkNonExpiringBasicLicense() throws Exception {
        Response licenseResponse = client().performRequest(new Request("GET", "/_license"));
        Map<String, Object> licenseResponseMap = entityAsMap(licenseResponse);
        Map<String, Object> licenseMap = (Map<String, Object>) licenseResponseMap.get("license");
        assertEquals("basic", licenseMap.get("type"));
        assertEquals("active", licenseMap.get("status"));
        assertNull(licenseMap.get("expiry_date_in_millis"));
    }
}
