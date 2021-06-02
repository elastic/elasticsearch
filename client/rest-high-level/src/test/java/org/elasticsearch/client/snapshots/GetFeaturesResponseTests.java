/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.snapshots;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.client.feature.GetFeaturesResponse;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;

public class GetFeaturesResponseTests extends AbstractResponseTestCase<
    org.elasticsearch.action.admin.cluster.snapshots.features.GetSnapshottableFeaturesResponse, GetFeaturesResponse> {

    @Override
    protected org.elasticsearch.action.admin.cluster.snapshots.features.GetSnapshottableFeaturesResponse createServerTestInstance(
        XContentType xContentType
    ) {
        return new org.elasticsearch.action.admin.cluster.snapshots.features.GetSnapshottableFeaturesResponse(
            randomList(
                10,
                () -> new org.elasticsearch.action.admin.cluster.snapshots.features.GetSnapshottableFeaturesResponse.SnapshottableFeature(
                    randomAlphaOfLengthBetween(4, 10),
                    randomAlphaOfLengthBetween(5, 10)
                )
            )
        );
    }

    @Override
    protected GetFeaturesResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return GetFeaturesResponse.parse(parser);
    }

    @Override
    protected void assertInstances(
        org.elasticsearch.action.admin.cluster.snapshots.features.GetSnapshottableFeaturesResponse serverTestInstance,
        GetFeaturesResponse clientInstance
    ) {
        assertNotNull(serverTestInstance.getSnapshottableFeatures());
        assertNotNull(serverTestInstance.getSnapshottableFeatures());

        assertThat(clientInstance.getFeatures(), hasSize(serverTestInstance.getSnapshottableFeatures().size()));

        Map<String, String> clientFeatures = clientInstance.getFeatures()
            .stream()
            .collect(Collectors.toMap(f -> f.getFeatureName(), f -> f.getDescription()));
        Map<String, String> serverFeatures = serverTestInstance.getSnapshottableFeatures()
            .stream()
            .collect(Collectors.toMap(f -> f.getFeatureName(), f -> f.getDescription()));

        assertThat(clientFeatures.entrySet(), everyItem(is(in(serverFeatures.entrySet()))));
    }
}
