/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.migration;

import org.elasticsearch.Version;
import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

public class GetFeatureUpgradeStatusResponseTests extends AbstractResponseTestCase<
    org.elasticsearch.action.admin.cluster.migration.GetFeatureUpgradeStatusResponse,
    GetFeatureUpgradeStatusResponse> {

    /** Our constructor should convert nulls to empty lists */
    public void testConstructorHandlesNullLists() {
        GetFeatureUpgradeStatusResponse response = new GetFeatureUpgradeStatusResponse(null, "status");
        assertThat(response.getFeatureUpgradeStatuses(), notNullValue());
        assertThat(response.getFeatureUpgradeStatuses(), equalTo(Collections.emptyList()));

    }

    @Override
    protected org.elasticsearch.action.admin.cluster.migration.GetFeatureUpgradeStatusResponse createServerTestInstance(
        XContentType xContentType
    ) {
        return new org.elasticsearch.action.admin.cluster.migration.GetFeatureUpgradeStatusResponse(
            randomList(
                5,
                () -> new org.elasticsearch.action.admin.cluster.migration.GetFeatureUpgradeStatusResponse.FeatureUpgradeStatus(
                    randomAlphaOfLengthBetween(3, 20),
                    randomFrom(Version.CURRENT, Version.CURRENT.minimumCompatibilityVersion()),
                    randomFrom(org.elasticsearch.action.admin.cluster.migration.GetFeatureUpgradeStatusResponse.UpgradeStatus.values()),
                    randomList(
                        4,
                        () -> new org.elasticsearch.action.admin.cluster.migration.GetFeatureUpgradeStatusResponse.IndexInfo(
                            randomAlphaOfLengthBetween(3, 20),
                            randomFrom(Version.CURRENT, Version.CURRENT.minimumCompatibilityVersion()),
                            null
                        )
                    )
                )
            ),
            randomFrom(org.elasticsearch.action.admin.cluster.migration.GetFeatureUpgradeStatusResponse.UpgradeStatus.values())
        );
    }

    @Override
    protected GetFeatureUpgradeStatusResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return GetFeatureUpgradeStatusResponse.parse(parser);
    }

    @Override
    protected void assertInstances(
        org.elasticsearch.action.admin.cluster.migration.GetFeatureUpgradeStatusResponse serverTestInstance,
        GetFeatureUpgradeStatusResponse clientInstance
    ) {

        assertThat(clientInstance.getUpgradeStatus(), equalTo(serverTestInstance.getUpgradeStatus().toString()));

        assertNotNull(serverTestInstance.getFeatureUpgradeStatuses());
        assertNotNull(clientInstance.getFeatureUpgradeStatuses());

        assertThat(clientInstance.getFeatureUpgradeStatuses(), hasSize(serverTestInstance.getFeatureUpgradeStatuses().size()));

        for (int i = 0; i < clientInstance.getFeatureUpgradeStatuses().size(); i++) {
            org.elasticsearch.action.admin.cluster.migration.GetFeatureUpgradeStatusResponse.FeatureUpgradeStatus serverTestStatus =
                serverTestInstance.getFeatureUpgradeStatuses().get(i);
            GetFeatureUpgradeStatusResponse.FeatureUpgradeStatus clientStatus = clientInstance.getFeatureUpgradeStatuses().get(i);

            assertThat(clientStatus.getFeatureName(), equalTo(serverTestStatus.getFeatureName()));
            assertThat(clientStatus.getMinimumIndexVersion(), equalTo(serverTestStatus.getMinimumIndexVersion().toString()));
            assertThat(clientStatus.getUpgradeStatus(), equalTo(serverTestStatus.getUpgradeStatus().toString()));

            assertThat(clientStatus.getIndexVersions(), hasSize(serverTestStatus.getIndexVersions().size()));

            for (int j = 0; i < clientStatus.getIndexVersions().size(); i++) {
                org.elasticsearch.action.admin.cluster.migration.GetFeatureUpgradeStatusResponse.IndexInfo serverIndexInfo =
                    serverTestStatus.getIndexVersions().get(j);
                GetFeatureUpgradeStatusResponse.IndexVersion clientIndexVersion = clientStatus.getIndexVersions().get(j);

                assertThat(clientIndexVersion.getIndexName(), equalTo(serverIndexInfo.getIndexName()));
                assertThat(clientIndexVersion.getVersion(), equalTo(serverIndexInfo.getVersion().toString()));
            }
        }
    }
}
