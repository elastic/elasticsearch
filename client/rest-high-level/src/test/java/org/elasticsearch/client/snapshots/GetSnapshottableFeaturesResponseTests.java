/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.snapshots;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;

public class GetSnapshottableFeaturesResponseTests extends AbstractResponseTestCase<
    org.elasticsearch.action.admin.cluster.snapshots.features.GetSnapshottableFeaturesResponse,
    GetSnapshottableFeaturesResponse> {

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
    protected GetSnapshottableFeaturesResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return GetSnapshottableFeaturesResponse.parse(parser);
    }

    @Override
    protected void assertInstances(
        org.elasticsearch.action.admin.cluster.snapshots.features.GetSnapshottableFeaturesResponse serverTestInstance,
        GetSnapshottableFeaturesResponse clientInstance
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
