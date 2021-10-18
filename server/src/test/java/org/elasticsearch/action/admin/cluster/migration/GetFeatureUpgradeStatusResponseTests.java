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

import static org.elasticsearch.action.admin.cluster.migration.GetFeatureUpgradeStatusResponse.UpgradeStatus.UPGRADE_NEEDED;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Tests for the Get Feature Upgrade Status response object.
 */
public class GetFeatureUpgradeStatusResponseTests extends AbstractWireSerializingTestCase<GetFeatureUpgradeStatusResponse> {

    @Override
    protected Writeable.Reader<GetFeatureUpgradeStatusResponse> instanceReader() {
        return GetFeatureUpgradeStatusResponse::new;
    }

    @Override
    protected GetFeatureUpgradeStatusResponse createTestInstance() {
        return new GetFeatureUpgradeStatusResponse(
            randomList(8, GetFeatureUpgradeStatusResponseTests::createFeatureStatus),
            randomFrom(org.elasticsearch.action.admin.cluster.migration.GetFeatureUpgradeStatusResponse.UpgradeStatus.values())
        );
    }

    @Override
    protected GetFeatureUpgradeStatusResponse mutateInstance(GetFeatureUpgradeStatusResponse instance) throws IOException {
        return new GetFeatureUpgradeStatusResponse(
            randomList(8,
                () -> randomValueOtherThanMany(instance.getFeatureUpgradeStatuses()::contains,
                    GetFeatureUpgradeStatusResponseTests::createFeatureStatus)),
            randomValueOtherThan(instance.getUpgradeStatus(), () ->
                randomFrom(org.elasticsearch.action.admin.cluster.migration.GetFeatureUpgradeStatusResponse.UpgradeStatus.values())));

    }

    /** If constructor is called with null for a list, we just use an empty list */
    public void testConstructorHandlesNullLists() {
        GetFeatureUpgradeStatusResponse response = new GetFeatureUpgradeStatusResponse(null, UPGRADE_NEEDED);
        assertThat(response.getFeatureUpgradeStatuses(), notNullValue());
        assertThat(response.getFeatureUpgradeStatuses(), equalTo(Collections.emptyList()));
    }

    private static GetFeatureUpgradeStatusResponse.FeatureUpgradeStatus createFeatureStatus() {
        return new GetFeatureUpgradeStatusResponse.FeatureUpgradeStatus(
            randomAlphaOfLengthBetween(3, 20),
            randomFrom(Version.CURRENT, Version.CURRENT.minimumCompatibilityVersion()),
            randomFrom(org.elasticsearch.action.admin.cluster.migration.GetFeatureUpgradeStatusResponse.UpgradeStatus.values()),
            randomList(4, GetFeatureUpgradeStatusResponseTests::getIndexVersion)
        );
    }

    private static GetFeatureUpgradeStatusResponse.IndexVersion getIndexVersion() {
        return new GetFeatureUpgradeStatusResponse.IndexVersion(
            randomAlphaOfLengthBetween(3, 20),
            randomFrom(Version.CURRENT, Version.CURRENT.minimumCompatibilityVersion())
        );
    }
}
