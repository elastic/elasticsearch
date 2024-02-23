/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.discovery.ec2;

import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;

import static org.hamcrest.CoreMatchers.is;

/**
 * Just an empty Node Start test to check eveything if fine when
 * starting.
 * This test requires AWS to run.
 */
@ClusterScope(scope = Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class Ec2DiscoveryUpdateSettingsTests extends AbstractAwsTestCase {
    public void testMinimumMasterNodesStart() {
        Settings nodeSettings = Settings.builder().put(DiscoveryModule.DISCOVERY_SEED_PROVIDERS_SETTING.getKey(), "ec2").build();
        internalCluster().startNode(nodeSettings);

        // We try to update a setting now
        final String expectedValue = UUIDs.randomBase64UUID(random());
        final String settingName = "cluster.routing.allocation.exclude.any_attribute";
        final ClusterUpdateSettingsResponse response = clusterAdmin().prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put(settingName, expectedValue))
            .get();

        final String value = response.getPersistentSettings().get(settingName);
        assertThat(value, is(expectedValue));
    }
}
