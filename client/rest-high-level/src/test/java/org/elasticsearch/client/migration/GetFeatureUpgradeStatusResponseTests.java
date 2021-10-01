/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.migration;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

public class GetFeatureUpgradeStatusResponseTests extends AbstractResponseTestCase<
    org.elasticsearch.action.admin.cluster.migration.GetFeatureUpgradeStatusResponse, GetFeatureUpgradeStatusResponse> {

    /** Our constructor should convert nulls to empty lists */
    public void testConstructorHandlesNullLists() {
        GetFeatureUpgradeStatusResponse response = new GetFeatureUpgradeStatusResponse(null, "status");
        assertThat(response.getFeatureUpgradeStatuses(), notNullValue());
        assertThat(response.getFeatureUpgradeStatuses(), equalTo(Collections.emptyList()));

    }

    @Override
    protected org.elasticsearch.action.admin.cluster.migration.GetFeatureUpgradeStatusResponse createServerTestInstance(
        XContentType xContentType) {
        return new org.elasticsearch.action.admin.cluster.migration.GetFeatureUpgradeStatusResponse(
            randomList(5,
                () -> new org.elasticsearch.action.admin.cluster.migration.GetFeatureUpgradeStatusResponse.FeatureUpgradeStatus(
                    randomAlphaOfLengthBetween(3, 20),
                    randomAlphaOfLengthBetween(5, 9),
                    randomAlphaOfLengthBetween(4, 16),
                    randomList(4,
                        () -> new org.elasticsearch.action.admin.cluster.migration.GetFeatureUpgradeStatusResponse.IndexVersion(
                            randomAlphaOfLengthBetween(3, 20),
                            randomAlphaOfLengthBetween(5, 9)))
                )),
            randomAlphaOfLength(5)
        );
    }

    @Override
    protected GetFeatureUpgradeStatusResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return GetFeatureUpgradeStatusResponse.parse(parser);
    }

    @Override
    protected void assertInstances(
        org.elasticsearch.action.admin.cluster.migration.GetFeatureUpgradeStatusResponse serverTestInstance,
        GetFeatureUpgradeStatusResponse clientInstance) {

        assertThat(clientInstance.getUpgradeStatus(), equalTo(serverTestInstance.getUpgradeStatus()));

        assertNotNull(serverTestInstance.getFeatureUpgradeStatuses());
        assertNotNull(clientInstance.getFeatureUpgradeStatuses());

        assertThat(clientInstance.getFeatureUpgradeStatuses(), hasSize(serverTestInstance.getFeatureUpgradeStatuses().size()));

        for (int i = 0; i < clientInstance.getFeatureUpgradeStatuses().size(); i++) {
            org.elasticsearch.action.admin.cluster.migration.GetFeatureUpgradeStatusResponse.FeatureUpgradeStatus serverTestStatus
                = serverTestInstance.getFeatureUpgradeStatuses().get(i);
            GetFeatureUpgradeStatusResponse.FeatureUpgradeStatus clientStatus = clientInstance.getFeatureUpgradeStatuses().get(i);

            assertThat(clientStatus.getFeatureName(), equalTo(serverTestStatus.getFeatureName()));
            assertThat(clientStatus.getMinimumIndexVersion(), equalTo(serverTestStatus.getMinimumIndexVersion()));
            assertThat(clientStatus.getUpgradeStatus(), equalTo(serverTestStatus.getUpgradeStatus()));

            assertThat(clientStatus.getIndexVersions(), hasSize(serverTestStatus.getIndexVersions().size()));

            for (int j = 0; i < clientStatus.getIndexVersions().size(); i++) {
                org.elasticsearch.action.admin.cluster.migration.GetFeatureUpgradeStatusResponse.IndexVersion serverIndexVersion
                    = serverTestStatus.getIndexVersions().get(j);
                GetFeatureUpgradeStatusResponse.IndexVersion clientIndexVersion = clientStatus.getIndexVersions().get(j);

                assertThat(clientIndexVersion.getIndexName(), equalTo(serverIndexVersion.getIndexName()));
                assertThat(clientIndexVersion.getVersion(), equalTo(serverIndexVersion.getVersion()));
            }
        }
    }
}
