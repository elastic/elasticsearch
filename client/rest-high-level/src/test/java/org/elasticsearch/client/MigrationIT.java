/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.elasticsearch.client.migration.DeprecationInfoRequest;
import org.elasticsearch.client.migration.DeprecationInfoResponse;
import org.elasticsearch.client.migration.GetFeatureUpgradeStatusRequest;
import org.elasticsearch.client.migration.GetFeatureUpgradeStatusResponse;
import org.elasticsearch.client.migration.PostFeatureUpgradeRequest;
import org.elasticsearch.client.migration.PostFeatureUpgradeResponse;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class MigrationIT extends ESRestHighLevelClientTestCase {

    public void testGetDeprecationInfo() throws IOException {
        createIndex("test", Settings.EMPTY);
        DeprecationInfoRequest request = new DeprecationInfoRequest(Collections.singletonList("test"));
        DeprecationInfoResponse response = highLevelClient().migration().getDeprecationInfo(request, RequestOptions.DEFAULT);
        // a test like this cannot test actual deprecations
        assertThat(response.getClusterSettingsIssues().size(), equalTo(0));
        assertThat(response.getIndexSettingsIssues().size(), equalTo(0));
        assertThat(response.getNodeSettingsIssues().size(), equalTo(0));
        assertThat(response.getMlSettingsIssues().size(), equalTo(0));
    }

    public void testGetFeatureUpgradeStatus() throws IOException {
        GetFeatureUpgradeStatusRequest request = new GetFeatureUpgradeStatusRequest();
        GetFeatureUpgradeStatusResponse response = highLevelClient().migration().getFeatureUpgradeStatus(request, RequestOptions.DEFAULT);
        assertThat(response.getUpgradeStatus(), equalTo("UPGRADE_NEEDED"));
        assertThat(response.getFeatureUpgradeStatuses().size(), equalTo(1));
        GetFeatureUpgradeStatusResponse.FeatureUpgradeStatus status = response.getFeatureUpgradeStatuses().get(0);
        assertThat(status.getUpgradeStatus(), equalTo("UPGRADE_NEEDED"));
        assertThat(status.getMinimumIndexVersion(), equalTo("7.1.1"));
        assertThat(status.getFeatureName(), equalTo("security"));
        assertThat(status.getIndexVersions().size(), equalTo(1));
    }

    public void testPostFeatureUpgradeStatus() throws IOException {
        PostFeatureUpgradeRequest request = new PostFeatureUpgradeRequest();
        PostFeatureUpgradeResponse response = highLevelClient().migration().postFeatureUpgrade(request, RequestOptions.DEFAULT);
        // a test like this cannot test actual deprecations
        assertThat(response.isAccepted(), equalTo(true));
        assertThat(response.getFeatures().size(), equalTo(1));
        PostFeatureUpgradeResponse.Feature feature = response.getFeatures().get(0);
        assertThat(feature.getFeatureName(), equalTo("security"));
        assertThat(response.getReason(), nullValue());
        assertThat(response.getElasticsearchException(), nullValue());
    }
}
