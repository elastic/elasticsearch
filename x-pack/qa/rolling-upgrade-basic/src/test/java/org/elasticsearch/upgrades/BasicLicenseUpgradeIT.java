/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.upgrades;

import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;

import java.io.IOException;
import java.util.Map;

public class BasicLicenseUpgradeIT extends AbstractUpgradeTestCase {

    @Override
    protected boolean preserveSystemResources() {
        // bug in the ML reset API before v8.7
        try {
            if (minimumNodeVersion().before(Version.V_8_7_0)) {
                return true;
            }
        } catch (IOException e) {
            return true;
        }
        return false;
    }

    public void testOldAndMixedClusterHaveActiveBasic() throws Exception {
        assumeTrue("only runs against old or mixed cluster", clusterType == CLUSTER_TYPE.OLD || clusterType == CLUSTER_TYPE.MIXED);
        assertBusy(this::checkBasicLicense);
    }

    public void testNewClusterHasActiveNonExpiringBasic() throws Exception {
        assumeTrue("only runs against upgraded cluster", clusterType == CLUSTER_TYPE.UPGRADED);
        assertBusy(this::checkNonExpiringBasicLicense);
    }

    @SuppressWarnings("unchecked")
    private void checkBasicLicense() throws Exception {
        final Request request = new Request("GET", "/_license");
        // This avoids throwing a ResponseException when the license is not ready yet
        // allowing to retry the check using assertBusy
        request.addParameter("ignore", "404");
        Response licenseResponse = client().performRequest(request);
        assertOK(licenseResponse);
        Map<String, Object> licenseResponseMap = entityAsMap(licenseResponse);
        Map<String, Object> licenseMap = (Map<String, Object>) licenseResponseMap.get("license");
        assertEquals("basic", licenseMap.get("type"));
        assertEquals("active", licenseMap.get("status"));
    }

    @SuppressWarnings("unchecked")
    private void checkNonExpiringBasicLicense() throws Exception {
        final Request request = new Request("GET", "/_license");
        // This avoids throwing a ResponseException when the license is not ready yet
        // allowing to retry the check using assertBusy
        request.addParameter("ignore", "404");
        Response licenseResponse = client().performRequest(request);
        assertOK(licenseResponse);
        Map<String, Object> licenseResponseMap = entityAsMap(licenseResponse);
        Map<String, Object> licenseMap = (Map<String, Object>) licenseResponseMap.get("license");
        assertEquals("basic", licenseMap.get("type"));
        assertEquals("active", licenseMap.get("status"));
        assertNull(licenseMap.get("expiry_date_in_millis"));
    }
}
