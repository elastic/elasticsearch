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
import java.util.List;

public class GetFeatureUpgradeStatusResponseTest extends AbstractWireSerializingTestCase<GetFeatureUpgradeStatusResponse> {

    @Override
    protected Writeable.Reader<GetFeatureUpgradeStatusResponse> instanceReader() {
        return GetFeatureUpgradeStatusResponse::new;
    }

    @Override
    protected GetFeatureUpgradeStatusResponse createTestInstance() {
        return new GetFeatureUpgradeStatusResponse(
            List.of(),
            randomAlphaOfLengthBetween(4, 16)
        );
    }

    @Override
    protected GetFeatureUpgradeStatusResponse mutateInstance(GetFeatureUpgradeStatusResponse instance) throws IOException {
        return new GetFeatureUpgradeStatusResponse(
            randomList(8, GetFeatureUpgradeStatusResponseTest::createFeatureStatus),
            randomValueOtherThan(instance.upgradeStatus(), () -> randomAlphaOfLengthBetween(4, 16))
        );
    }

    private static GetFeatureUpgradeStatusResponse.FeatureUpgradeStatus createFeatureStatus() {
        return new GetFeatureUpgradeStatusResponse.FeatureUpgradeStatus(
            randomAlphaOfLengthBetween(3, 20),
            randomAlphaOfLengthBetween(5, 9),
            randomAlphaOfLengthBetween(4, 16),
            randomList(4, GetFeatureUpgradeStatusResponseTest::getIndexVersion)
        );
    }

    private static GetFeatureUpgradeStatusResponse.IndexVersion getIndexVersion() {
        return new GetFeatureUpgradeStatusResponse.IndexVersion(
            randomAlphaOfLengthBetween(3, 20),
            randomAlphaOfLengthBetween(5, 9)
        );
    }
}
