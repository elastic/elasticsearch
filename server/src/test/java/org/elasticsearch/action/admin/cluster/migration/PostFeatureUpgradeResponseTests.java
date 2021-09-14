/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.migration;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;

public class PostFeatureUpgradeResponseTest extends AbstractWireSerializingTestCase<PostFeatureUpgradeResponse> {

    @Override
    protected Writeable.Reader<PostFeatureUpgradeResponse> instanceReader() {
        return PostFeatureUpgradeResponse::new;
    }

    @Override
    protected PostFeatureUpgradeResponse createTestInstance() {
        if (randomBoolean()) {
            return new PostFeatureUpgradeResponse(
                true,
                randomList(1, 9, PostFeatureUpgradeResponseTest::createFeature),
                null,
                null);
        }
        return new PostFeatureUpgradeResponse(false,
            new ArrayList<>(),
            randomAlphaOfLengthBetween(10, 30),
            new ElasticsearchException(randomAlphaOfLengthBetween(10, 30)));
    }

    @Override
    protected PostFeatureUpgradeResponse mutateInstance(PostFeatureUpgradeResponse instance) throws IOException {
        if (instance.isAccepted()) {
            return new PostFeatureUpgradeResponse(
                true,
                randomList(1, 9, () ->
                    randomValueOtherThanMany(instance.getFeatures()::contains, PostFeatureUpgradeResponseTest::createFeature)),
                null,
                null);
        }
        return new PostFeatureUpgradeResponse(false,
            new ArrayList<>(),
            randomValueOtherThan(instance.getReason(), () -> randomAlphaOfLengthBetween(10, 30)),
            instance.getElasticsearchException());
    }

    private static PostFeatureUpgradeResponse.Feature createFeature() {
        return new PostFeatureUpgradeResponse.Feature(randomAlphaOfLengthBetween(4, 12));
    }
}
