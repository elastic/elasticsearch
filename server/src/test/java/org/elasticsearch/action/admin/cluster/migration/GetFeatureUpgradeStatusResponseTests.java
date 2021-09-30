/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.migration;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.Collections;

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
            randomAlphaOfLengthBetween(4, 16)
        );
    }

    @Override
    protected GetFeatureUpgradeStatusResponse mutateInstance(GetFeatureUpgradeStatusResponse instance) throws IOException {
        return new GetFeatureUpgradeStatusResponse(
            randomList(8,
                () -> randomValueOtherThanMany(instance.getFeatureUpgradeStatuses()::contains,
                    GetFeatureUpgradeStatusResponseTests::createFeatureStatus)),
            randomValueOtherThan(instance.getUpgradeStatus(), () -> randomAlphaOfLengthBetween(4, 16))
        );
    }

    /** If constructor is called with null for a list, we just use an empty list */
    public void testConstructorHandlesNullLists() {
        GetFeatureUpgradeStatusResponse response = new GetFeatureUpgradeStatusResponse(null, "status");
        assertThat(response.getFeatureUpgradeStatuses(), notNullValue());
        assertThat(response.getFeatureUpgradeStatuses(), equalTo(Collections.emptyList()));
    }

    private static GetFeatureUpgradeStatusResponse.FeatureUpgradeStatus createFeatureStatus() {
        return new GetFeatureUpgradeStatusResponse.FeatureUpgradeStatus(
            randomAlphaOfLengthBetween(3, 20),
            randomAlphaOfLengthBetween(5, 9),
            randomAlphaOfLengthBetween(4, 16),
            randomList(4, GetFeatureUpgradeStatusResponseTests::getIndexVersion)
        );
    }

    private static GetFeatureUpgradeStatusResponse.IndexVersion getIndexVersion() {
        return new GetFeatureUpgradeStatusResponse.IndexVersion(
            randomAlphaOfLengthBetween(3, 20),
            randomAlphaOfLengthBetween(5, 9)
        );
    }
}
