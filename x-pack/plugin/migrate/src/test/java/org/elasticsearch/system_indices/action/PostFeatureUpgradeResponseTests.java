/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.system_indices.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.ArrayList;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Tests for the Post Feature Upgrade response object.
 */
public class PostFeatureUpgradeResponseTests extends AbstractWireSerializingTestCase<PostFeatureUpgradeResponse> {

    @Override
    protected Writeable.Reader<PostFeatureUpgradeResponse> instanceReader() {
        return PostFeatureUpgradeResponse::new;
    }

    @Override
    protected PostFeatureUpgradeResponse createTestInstance() {
        if (randomBoolean()) {
            return new PostFeatureUpgradeResponse(true, randomList(9, PostFeatureUpgradeResponseTests::createFeature), null, null);
        }
        return new PostFeatureUpgradeResponse(
            false,
            new ArrayList<>(),
            randomAlphaOfLengthBetween(10, 30),
            new ElasticsearchException(randomAlphaOfLengthBetween(10, 30))
        );
    }

    @Override
    protected PostFeatureUpgradeResponse mutateInstance(PostFeatureUpgradeResponse instance) {
        if (instance.isAccepted()) {
            return new PostFeatureUpgradeResponse(
                true,
                randomList(
                    1,
                    9,
                    () -> randomValueOtherThanMany(instance.getFeatures()::contains, PostFeatureUpgradeResponseTests::createFeature)
                ),
                null,
                null
            );
        }
        return new PostFeatureUpgradeResponse(
            false,
            new ArrayList<>(),
            randomValueOtherThan(instance.getReason(), () -> randomAlphaOfLengthBetween(10, 30)),
            instance.getElasticsearchException()
        );
    }

    /** If constructor is called with null for a list, we just use an empty list */
    public void testConstructorHandlesNullLists() {
        PostFeatureUpgradeResponse response = new PostFeatureUpgradeResponse(true, null, null, null);
        assertThat(response.getFeatures(), notNullValue());
        assertThat(response.getFeatures(), equalTo(Collections.emptyList()));
    }

    private static PostFeatureUpgradeResponse.Feature createFeature() {
        return new PostFeatureUpgradeResponse.Feature(randomAlphaOfLengthBetween(4, 12));
    }
}
