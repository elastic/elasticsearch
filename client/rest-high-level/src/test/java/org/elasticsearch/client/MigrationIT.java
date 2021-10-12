/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.elasticsearch.Version;
import org.elasticsearch.client.migration.DeprecationInfoRequest;
import org.elasticsearch.client.migration.DeprecationInfoResponse;
import org.elasticsearch.client.migration.GetFeatureMigrationStatusRequest;
import org.elasticsearch.client.migration.GetFeatureMigrationStatusResponse;
import org.elasticsearch.client.migration.PostFeatureUpgradeRequest;
import org.elasticsearch.client.migration.PostFeatureUpgradeResponse;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
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

    public void testGetFeatureMigrationStatus() throws IOException {
        GetFeatureMigrationStatusRequest request = new GetFeatureMigrationStatusRequest();
        GetFeatureMigrationStatusResponse response = highLevelClient().migration()
            .getFeatureMigrationStatus(request, RequestOptions.DEFAULT);
        assertThat(response.getMigrationStatus(), equalTo("NO_MIGRATION_NEEDED"));
        assertThat(response.getFeatureMigrationStatuses().size(), greaterThanOrEqualTo(1));
        Optional<GetFeatureMigrationStatusResponse.FeatureMigrationStatus> optionalTasksStatus = response.getFeatureMigrationStatuses()
            .stream()
            .filter(status -> "tasks".equals(status.getFeatureName()))
            .findFirst();

        assertThat(optionalTasksStatus.isPresent(), is(true));

        GetFeatureMigrationStatusResponse.FeatureMigrationStatus tasksStatus = optionalTasksStatus.get();

        assertThat(tasksStatus.getMigrationStatus(), equalTo("NO_MIGRATION_NEEDED"));
        assertThat(tasksStatus.getMinimumIndexVersion(), equalTo(Version.CURRENT.toString()));
        assertThat(tasksStatus.getFeatureName(), equalTo("tasks"));
    }

    public void testPostFeatureUpgradeStatus() throws IOException {
        PostFeatureUpgradeRequest request = new PostFeatureUpgradeRequest();
        PostFeatureUpgradeResponse response = highLevelClient().migration().postFeatureUpgrade(request, RequestOptions.DEFAULT);
        assertThat(response.isAccepted(), equalTo(true));
        assertThat(response.getFeatures().size(), equalTo(1));
        PostFeatureUpgradeResponse.Feature feature = response.getFeatures().get(0);
        assertThat(feature.getFeatureName(), equalTo("security"));
        assertThat(response.getReason(), nullValue());
        assertThat(response.getElasticsearchException(), nullValue());
    }
}
