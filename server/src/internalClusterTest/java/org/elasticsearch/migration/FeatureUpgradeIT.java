/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.migration;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.migration.GetFeatureUpgradeStatusAction;
import org.elasticsearch.action.admin.cluster.migration.GetFeatureUpgradeStatusRequest;
import org.elasticsearch.action.admin.cluster.migration.GetFeatureUpgradeStatusResponse;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class FeatureUpgradeIT extends ESIntegTestCase {

    public void testGetFeatureUpgradeStatus() throws Exception {
        GetFeatureUpgradeStatusResponse apiResponse = client().execute(
            GetFeatureUpgradeStatusAction.INSTANCE, new GetFeatureUpgradeStatusRequest())
            .get();

        GetFeatureUpgradeStatusResponse.FeatureUpgradeStatus status = new GetFeatureUpgradeStatusResponse.FeatureUpgradeStatus(
            "tasks",
            Version.CURRENT.toString(),
            "NO_UPGRADE_NEEDED",
            List.of()
        );
        assertThat(apiResponse, equalTo(
           new GetFeatureUpgradeStatusResponse(
               List.of(status),
               "NO_UPGRADE_NEEDED"
           )));
    }
}
