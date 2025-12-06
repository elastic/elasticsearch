/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.system_indices.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.Collections;

import static org.elasticsearch.system_indices.action.GetFeatureUpgradeStatusResponse.UpgradeStatus.ERROR;
import static org.elasticsearch.system_indices.action.GetFeatureUpgradeStatusResponse.UpgradeStatus.IN_PROGRESS;
import static org.elasticsearch.system_indices.action.GetFeatureUpgradeStatusResponse.UpgradeStatus.MIGRATION_NEEDED;
import static org.elasticsearch.system_indices.action.GetFeatureUpgradeStatusResponse.UpgradeStatus.NO_MIGRATION_NEEDED;
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
            randomFrom(GetFeatureUpgradeStatusResponse.UpgradeStatus.values())
        );
    }

    @Override
    protected GetFeatureUpgradeStatusResponse mutateInstance(GetFeatureUpgradeStatusResponse instance) {
        return new GetFeatureUpgradeStatusResponse(
            randomList(
                8,
                () -> randomValueOtherThanMany(
                    instance.getFeatureUpgradeStatuses()::contains,
                    GetFeatureUpgradeStatusResponseTests::createFeatureStatus
                )
            ),
            randomValueOtherThan(instance.getUpgradeStatus(), () -> randomFrom(GetFeatureUpgradeStatusResponse.UpgradeStatus.values()))
        );

    }

    /** If constructor is called with null for a list, we just use an empty list */
    public void testConstructorHandlesNullLists() {
        GetFeatureUpgradeStatusResponse response = new GetFeatureUpgradeStatusResponse(null, MIGRATION_NEEDED);
        assertThat(response.getFeatureUpgradeStatuses(), notNullValue());
        assertThat(response.getFeatureUpgradeStatuses(), equalTo(Collections.emptyList()));
    }

    public void testUpgradeStatusCominations() {
        assertEquals(NO_MIGRATION_NEEDED, GetFeatureUpgradeStatusResponse.UpgradeStatus.combine(NO_MIGRATION_NEEDED, NO_MIGRATION_NEEDED));

        assertEquals(MIGRATION_NEEDED, GetFeatureUpgradeStatusResponse.UpgradeStatus.combine(NO_MIGRATION_NEEDED, MIGRATION_NEEDED));
        assertEquals(MIGRATION_NEEDED, GetFeatureUpgradeStatusResponse.UpgradeStatus.combine(MIGRATION_NEEDED, NO_MIGRATION_NEEDED));
        assertEquals(MIGRATION_NEEDED, GetFeatureUpgradeStatusResponse.UpgradeStatus.combine(MIGRATION_NEEDED, MIGRATION_NEEDED));

        assertEquals(IN_PROGRESS, GetFeatureUpgradeStatusResponse.UpgradeStatus.combine(IN_PROGRESS, NO_MIGRATION_NEEDED));
        assertEquals(IN_PROGRESS, GetFeatureUpgradeStatusResponse.UpgradeStatus.combine(NO_MIGRATION_NEEDED, IN_PROGRESS));
        assertEquals(IN_PROGRESS, GetFeatureUpgradeStatusResponse.UpgradeStatus.combine(MIGRATION_NEEDED, IN_PROGRESS));
        assertEquals(IN_PROGRESS, GetFeatureUpgradeStatusResponse.UpgradeStatus.combine(IN_PROGRESS, MIGRATION_NEEDED));
        assertEquals(IN_PROGRESS, GetFeatureUpgradeStatusResponse.UpgradeStatus.combine(IN_PROGRESS, IN_PROGRESS));

        assertEquals(ERROR, GetFeatureUpgradeStatusResponse.UpgradeStatus.combine(ERROR, NO_MIGRATION_NEEDED));
        assertEquals(ERROR, GetFeatureUpgradeStatusResponse.UpgradeStatus.combine(NO_MIGRATION_NEEDED, ERROR));
        assertEquals(ERROR, GetFeatureUpgradeStatusResponse.UpgradeStatus.combine(MIGRATION_NEEDED, ERROR));
        assertEquals(ERROR, GetFeatureUpgradeStatusResponse.UpgradeStatus.combine(ERROR, MIGRATION_NEEDED));
        assertEquals(ERROR, GetFeatureUpgradeStatusResponse.UpgradeStatus.combine(IN_PROGRESS, ERROR));
        assertEquals(ERROR, GetFeatureUpgradeStatusResponse.UpgradeStatus.combine(ERROR, IN_PROGRESS));
        assertEquals(ERROR, GetFeatureUpgradeStatusResponse.UpgradeStatus.combine(ERROR, ERROR));
    }

    private static GetFeatureUpgradeStatusResponse.FeatureUpgradeStatus createFeatureStatus() {
        return new GetFeatureUpgradeStatusResponse.FeatureUpgradeStatus(
            randomAlphaOfLengthBetween(3, 20),
            randomFrom(IndexVersion.current(), IndexVersions.MINIMUM_COMPATIBLE),
            randomFrom(GetFeatureUpgradeStatusResponse.UpgradeStatus.values()),
            randomList(4, GetFeatureUpgradeStatusResponseTests::getIndexInfo)
        );
    }

    private static GetFeatureUpgradeStatusResponse.IndexInfo getIndexInfo() {
        return new GetFeatureUpgradeStatusResponse.IndexInfo(
            randomAlphaOfLengthBetween(3, 20),
            randomFrom(IndexVersion.current(), IndexVersions.MINIMUM_COMPATIBLE),
            null
        );
    }
}
