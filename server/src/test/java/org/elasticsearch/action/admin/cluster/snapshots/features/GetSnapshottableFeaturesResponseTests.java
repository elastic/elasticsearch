/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.features;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;

public class GetSnapshottableFeaturesResponseTests extends AbstractWireSerializingTestCase<GetSnapshottableFeaturesResponse> {

    @Override
    protected Writeable.Reader<GetSnapshottableFeaturesResponse> instanceReader() {
        return GetSnapshottableFeaturesResponse::new;
    }

    @Override
    protected GetSnapshottableFeaturesResponse createTestInstance() {
        return new GetSnapshottableFeaturesResponse(
            randomList(
                10,
                () -> new GetSnapshottableFeaturesResponse.SnapshottableFeature(
                    randomAlphaOfLengthBetween(4, 10),
                    randomAlphaOfLengthBetween(5, 10)
                )
            )
        );
    }

    @Override
    protected GetSnapshottableFeaturesResponse mutateInstance(GetSnapshottableFeaturesResponse instance) throws IOException {
        int minSize = 0;
        if (instance.getSnapshottableFeatures().size() == 0) {
            minSize = 1;
        }
        Set<String> existingFeatureNames = instance.getSnapshottableFeatures()
            .stream()
            .map(feature -> feature.getFeatureName())
            .collect(Collectors.toSet());
        return new GetSnapshottableFeaturesResponse(
            randomList(
                minSize,
                10,
                () -> new GetSnapshottableFeaturesResponse.SnapshottableFeature(
                    randomValueOtherThanMany(existingFeatureNames::contains, () -> randomAlphaOfLengthBetween(4, 10)),
                    randomAlphaOfLengthBetween(5, 10)
                )
            )
        );
    }
}
