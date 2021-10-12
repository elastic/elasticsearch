/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.migration;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.Collections;

import static org.elasticsearch.action.admin.cluster.migration.GetFeatureMigrationStatusResponse.MigrationStatus.MIGRATION_NEEDED;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Tests for the Get Feature Upgrade Status response object.
 */
public class GetFeatureMigrationStatusResponseTests extends AbstractWireSerializingTestCase<GetFeatureMigrationStatusResponse> {

    @Override
    protected Writeable.Reader<GetFeatureMigrationStatusResponse> instanceReader() {
        return GetFeatureMigrationStatusResponse::new;
    }

    @Override
    protected GetFeatureMigrationStatusResponse createTestInstance() {
        return new GetFeatureMigrationStatusResponse(
            randomList(8, GetFeatureMigrationStatusResponseTests::createFeatureStatus),
            randomFrom(GetFeatureMigrationStatusResponse.MigrationStatus.values())
        );
    }

    @Override
    protected GetFeatureMigrationStatusResponse mutateInstance(GetFeatureMigrationStatusResponse instance) throws IOException {
        return new GetFeatureMigrationStatusResponse(
            randomList(8,
                () -> randomValueOtherThanMany(instance.getFeatureMigrationStatuses()::contains,
                    GetFeatureMigrationStatusResponseTests::createFeatureStatus)),
            randomValueOtherThan(instance.getMigrationStatus(), () ->
                randomFrom(GetFeatureMigrationStatusResponse.MigrationStatus.values())));

    }

    /** If constructor is called with null for a list, we just use an empty list */
    public void testConstructorHandlesNullLists() {
        GetFeatureMigrationStatusResponse response = new GetFeatureMigrationStatusResponse(null, MIGRATION_NEEDED);
        assertThat(response.getFeatureMigrationStatuses(), notNullValue());
        assertThat(response.getFeatureMigrationStatuses(), equalTo(Collections.emptyList()));
    }

    private static GetFeatureMigrationStatusResponse.FeatureMigrationStatus createFeatureStatus() {
        return new GetFeatureMigrationStatusResponse.FeatureMigrationStatus(
            randomAlphaOfLengthBetween(3, 20),
            randomFrom(Version.CURRENT, Version.CURRENT.minimumCompatibilityVersion()),
            randomFrom(GetFeatureMigrationStatusResponse.MigrationStatus.values()),
            randomList(4, GetFeatureMigrationStatusResponseTests::getIndexVersion)
        );
    }

    private static GetFeatureMigrationStatusResponse.IndexVersion getIndexVersion() {
        return new GetFeatureMigrationStatusResponse.IndexVersion(
            randomAlphaOfLengthBetween(3, 20),
            randomFrom(Version.CURRENT, Version.CURRENT.minimumCompatibilityVersion())
        );
    }
}
