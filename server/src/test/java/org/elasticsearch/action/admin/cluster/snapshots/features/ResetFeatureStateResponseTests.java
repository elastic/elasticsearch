/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.features;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ResetFeatureStateResponseTests extends AbstractWireSerializingTestCase<ResetFeatureStateResponse> {

    @Override
    protected Writeable.Reader<ResetFeatureStateResponse> instanceReader() {
        return ResetFeatureStateResponse::new;
    }

    @Override
    protected ResetFeatureStateResponse createTestInstance() {
        List<ResetFeatureStateResponse.ResetFeatureStateStatus> resetStatuses = new ArrayList<>();
        String feature1 = randomAlphaOfLengthBetween(4, 10);
        String feature2 = randomValueOtherThan(feature1, () -> randomAlphaOfLengthBetween(4, 10));
        resetStatuses.add(
            randomFrom(
                ResetFeatureStateResponse.ResetFeatureStateStatus.success(feature1),
                ResetFeatureStateResponse.ResetFeatureStateStatus.failure(feature1, new ElasticsearchException("bad"))
            )
        );
        resetStatuses.add(
            randomFrom(
                ResetFeatureStateResponse.ResetFeatureStateStatus.success(feature2),
                ResetFeatureStateResponse.ResetFeatureStateStatus.failure(feature2, new ElasticsearchException("bad"))
            )
        );
        return new ResetFeatureStateResponse(resetStatuses);
    }

    @Override
    protected ResetFeatureStateResponse mutateInstance(ResetFeatureStateResponse instance) throws IOException {
        int minSize = 0;
        if (instance.getFeatureStateResetStatuses().size() == 0) {
            minSize = 1;
        }
        Set<String> existingFeatureNames = instance.getFeatureStateResetStatuses()
            .stream()
            .map(ResetFeatureStateResponse.ResetFeatureStateStatus::getFeatureName)
            .collect(Collectors.toSet());
        return new ResetFeatureStateResponse(
            randomList(
                minSize,
                10,
                () -> ResetFeatureStateResponse.ResetFeatureStateStatus.success(
                    randomValueOtherThanMany(existingFeatureNames::contains, () -> randomAlphaOfLengthBetween(4, 10))
                )
            )
        );
    }
}
