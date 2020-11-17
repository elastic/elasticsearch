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

package org.elasticsearch.action.admin.cluster.snapshots.features;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;

public class GetSnapshottableFeaturesResponseTests extends AbstractWireSerializingTestCase<GetSnapshottableFeaturesResponse> {

    @Override
    protected Writeable.Reader instanceReader() {
        return GetSnapshottableFeaturesResponse::new;
    }

    @Override
    protected GetSnapshottableFeaturesResponse createTestInstance() {
        return new GetSnapshottableFeaturesResponse(randomList(10,
            () -> new GetSnapshottableFeaturesResponse.SnapshottableFeature(randomAlphaOfLengthBetween(4, 10))));
    }

    @Override
    protected GetSnapshottableFeaturesResponse mutateInstance(GetSnapshottableFeaturesResponse instance) throws IOException {
        int minSize = 0;
        if (instance.getSnapshottableFeatures().size() == 0) {
            minSize = 1;
        }
        Set<String> existingFeatureNames = instance.getSnapshottableFeatures().stream()
            .map(feature -> feature.getFeatureName())
            .collect(Collectors.toSet());

        return new GetSnapshottableFeaturesResponse(randomList(minSize, 10,
            () -> new GetSnapshottableFeaturesResponse.SnapshottableFeature(
                randomValueOtherThanMany(existingFeatureNames::contains, () -> randomAlphaOfLengthBetween(4, 10)))));
    }
}
