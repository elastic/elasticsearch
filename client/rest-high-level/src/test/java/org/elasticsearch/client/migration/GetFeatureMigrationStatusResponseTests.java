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

public class GetFeatureMigrationStatusResponseTests extends AbstractResponseTestCase<
    org.elasticsearch.action.admin.cluster.migration.GetFeatureMigrationStatusResponse, GetFeatureMigrationStatusResponse> {

    /** Our constructor should convert nulls to empty lists */
    public void testConstructorHandlesNullLists() {
        GetFeatureMigrationStatusResponse response = new GetFeatureMigrationStatusResponse(null, "status");
        assertThat(response.getFeatureMigrationStatuses(), notNullValue());
        assertThat(response.getFeatureMigrationStatuses(), equalTo(Collections.emptyList()));

    }

    @Override
    protected org.elasticsearch.action.admin.cluster.migration.GetFeatureMigrationStatusResponse createServerTestInstance(
        XContentType xContentType) {
        return new org.elasticsearch.action.admin.cluster.migration.GetFeatureMigrationStatusResponse(
            randomList(5,
                () -> new org.elasticsearch.action.admin.cluster.migration.GetFeatureMigrationStatusResponse.FeatureMigrationStatus(
                    randomAlphaOfLengthBetween(3, 20),
                    randomFrom(Version.CURRENT, Version.CURRENT.minimumCompatibilityVersion()),
                    randomFrom(org.elasticsearch.action.admin.cluster.migration.GetFeatureMigrationStatusResponse.MigrationStatus.values()),
                    randomList(4,
                        () -> new org.elasticsearch.action.admin.cluster.migration.GetFeatureMigrationStatusResponse.IndexVersion(
                            randomAlphaOfLengthBetween(3, 20),
                            randomFrom(Version.CURRENT, Version.CURRENT.minimumCompatibilityVersion())))
                )),
            randomFrom(org.elasticsearch.action.admin.cluster.migration.GetFeatureMigrationStatusResponse.MigrationStatus.values())
        );
    }

    @Override
    protected GetFeatureMigrationStatusResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return GetFeatureMigrationStatusResponse.parse(parser);
    }

    @Override
    protected void assertInstances(
        org.elasticsearch.action.admin.cluster.migration.GetFeatureMigrationStatusResponse serverTestInstance,
        GetFeatureMigrationStatusResponse clientInstance) {

        assertThat(clientInstance.getMigrationStatus(), equalTo(serverTestInstance.getMigrationStatus().toString()));

        assertNotNull(serverTestInstance.getFeatureMigrationStatuses());
        assertNotNull(clientInstance.getFeatureMigrationStatuses());

        assertThat(clientInstance.getFeatureMigrationStatuses(), hasSize(serverTestInstance.getFeatureMigrationStatuses().size()));

        for (int i = 0; i < clientInstance.getFeatureMigrationStatuses().size(); i++) {
            org.elasticsearch.action.admin.cluster.migration.GetFeatureMigrationStatusResponse.FeatureMigrationStatus serverTestStatus
                = serverTestInstance.getFeatureMigrationStatuses().get(i);
            GetFeatureMigrationStatusResponse.FeatureMigrationStatus clientStatus = clientInstance.getFeatureMigrationStatuses().get(i);

            assertThat(clientStatus.getFeatureName(), equalTo(serverTestStatus.getFeatureName()));
            assertThat(clientStatus.getMinimumIndexVersion(), equalTo(serverTestStatus.getMinimumIndexVersion().toString()));
            assertThat(clientStatus.getMigrationStatus(), equalTo(serverTestStatus.getMigrationStatus().toString()));

            assertThat(clientStatus.getIndexVersions(), hasSize(serverTestStatus.getIndexVersions().size()));

            for (int j = 0; i < clientStatus.getIndexVersions().size(); i++) {
                org.elasticsearch.action.admin.cluster.migration.GetFeatureMigrationStatusResponse.IndexVersion serverIndexVersion
                    = serverTestStatus.getIndexVersions().get(j);
                GetFeatureMigrationStatusResponse.IndexVersion clientIndexVersion = clientStatus.getIndexVersions().get(j);

                assertThat(clientIndexVersion.getIndexName(), equalTo(serverIndexVersion.getIndexName()));
                assertThat(clientIndexVersion.getVersion(), equalTo(serverIndexVersion.getVersion().toString()));
            }
        }
    }
}
