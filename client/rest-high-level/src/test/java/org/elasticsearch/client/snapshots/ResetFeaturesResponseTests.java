/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.snapshots;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.snapshots.features.ResetFeatureStateResponse;
import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.client.feature.ResetFeaturesResponse;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;

public class ResetFeaturesResponseTests extends AbstractResponseTestCase<ResetFeatureStateResponse, ResetFeaturesResponse> {

    @Override
    protected ResetFeatureStateResponse createServerTestInstance(
        XContentType xContentType) {
        return new org.elasticsearch.action.admin.cluster.snapshots.features.ResetFeatureStateResponse(
            randomList(
                10,
                () -> randomBoolean()
                    ? ResetFeatureStateResponse.ResetFeatureStateStatus.success(randomAlphaOfLengthBetween(6, 10))
                    : ResetFeatureStateResponse.ResetFeatureStateStatus.failure(
                        randomAlphaOfLengthBetween(6, 10), new ElasticsearchException("something went wrong"))
            )
        );
    }

    @Override
    protected ResetFeaturesResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return ResetFeaturesResponse.parse(parser);
    }

    @Override
    protected void assertInstances(ResetFeatureStateResponse serverTestInstance, ResetFeaturesResponse clientInstance) {

        assertNotNull(serverTestInstance.getFeatureStateResetStatuses());
        assertNotNull(clientInstance.getFeatureResetStatuses());

        assertThat(clientInstance.getFeatureResetStatuses(), hasSize(serverTestInstance.getFeatureStateResetStatuses().size()));

        Map<String, String> clientFeatures = clientInstance.getFeatureResetStatuses()
            .stream()
            .collect(Collectors.toMap(f -> f.getFeatureName(), f -> f.getStatus()));
        Map<String, String> serverFeatures = serverTestInstance.getFeatureStateResetStatuses()
            .stream()
            .collect(Collectors.toMap(f -> f.getFeatureName(), f -> f.getStatus().toString()));

        assertThat(clientFeatures.entrySet(), everyItem(is(in(serverFeatures.entrySet()))));
    }
}
