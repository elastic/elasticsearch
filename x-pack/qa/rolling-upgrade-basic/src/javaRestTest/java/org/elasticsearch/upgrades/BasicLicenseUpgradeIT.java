/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ParameterizedRollingUpgradeTestCase;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.junit.ClassRule;

import java.util.Map;

public class BasicLicenseUpgradeIT extends ParameterizedRollingUpgradeTestCase {

    @ClassRule
    public static final ElasticsearchCluster cluster = buildCluster();

    private static ElasticsearchCluster buildCluster() {
        return ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .version(getOldClusterVersion(), isOldClusterDetachedVersion())
            .nodes(NODE_NUM)
            .setting("xpack.security.enabled", "false")
            .setting("xpack.ml.enabled", "false")
            .setting("xpack.watcher.enabled", "false")
            .setting("xpack.license.self_generated.type", "basic")
            .build();
    }

    public BasicLicenseUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    @Override
    protected ElasticsearchCluster getUpgradeCluster() {
        return cluster;
    }

    public void testOldAndMixedClusterHaveActiveBasic() throws Exception {
        assumeTrue("only runs against old or mixed cluster", isOldCluster() || isMixedCluster());
        assertBusy(this::checkBasicLicense);
    }

    public void testNewClusterHasActiveNonExpiringBasic() throws Exception {
        assumeTrue("only runs against upgraded cluster", isUpgradedCluster());
        assertBusy(this::checkNonExpiringBasicLicense);
    }

    @SuppressWarnings("unchecked")
    private void checkBasicLicense() throws Exception {
        final Request request = new Request("GET", "/_license");
        // This avoids throwing a ResponseException when the license is not ready yet
        // allowing to retry the check using assertBusy
        setIgnoredErrorResponseCodes(request, RestStatus.NOT_FOUND);
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
        setIgnoredErrorResponseCodes(request, RestStatus.NOT_FOUND);
        Response licenseResponse = client().performRequest(request);
        assertOK(licenseResponse);
        Map<String, Object> licenseResponseMap = entityAsMap(licenseResponse);
        Map<String, Object> licenseMap = (Map<String, Object>) licenseResponseMap.get("license");
        assertEquals("basic", licenseMap.get("type"));
        assertEquals("active", licenseMap.get("status"));
        assertNull(licenseMap.get("expiry_date_in_millis"));
    }
}
