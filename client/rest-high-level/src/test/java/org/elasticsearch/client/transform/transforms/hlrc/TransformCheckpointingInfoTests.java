/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.transform.transforms.hlrc;

import org.elasticsearch.client.AbstractHlrcXContentTestCase;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpointingInfo;

import java.io.IOException;
import java.time.Instant;
import java.util.function.Predicate;

public class TransformCheckpointingInfoTests extends AbstractHlrcXContentTestCase<
        TransformCheckpointingInfo,
        org.elasticsearch.client.transform.transforms.TransformCheckpointingInfo> {

    public static TransformCheckpointingInfo fromHlrc(
            org.elasticsearch.client.transform.transforms.TransformCheckpointingInfo instance) {
        return new TransformCheckpointingInfo(
            TransformCheckpointStatsTests.fromHlrc(instance.getLast()),
            TransformCheckpointStatsTests.fromHlrc(instance.getNext()),
            instance.getOperationsBehind(),
            instance.getChangesLastDetectedAt());
    }

    @Override
    public org.elasticsearch.client.transform.transforms.TransformCheckpointingInfo doHlrcParseInstance(XContentParser parser) {
        return org.elasticsearch.client.transform.transforms.TransformCheckpointingInfo.fromXContent(parser);
    }

    @Override
    public TransformCheckpointingInfo convertHlrcToInternal(
            org.elasticsearch.client.transform.transforms.TransformCheckpointingInfo instance) {
        return fromHlrc(instance);
    }

    public static TransformCheckpointingInfo randomTransformCheckpointingInfo() {
        return new TransformCheckpointingInfo(
            TransformCheckpointStatsTests.randomTransformCheckpointStats(),
            TransformCheckpointStatsTests.randomTransformCheckpointStats(),
            randomNonNegativeLong(),
            randomBoolean() ? null : Instant.ofEpochMilli(randomNonNegativeLong()));
    }

    @Override
    protected TransformCheckpointingInfo createTestInstance() {
        return randomTransformCheckpointingInfo();
    }

    @Override
    protected TransformCheckpointingInfo doParseInstance(XContentParser parser) throws IOException {
        return TransformCheckpointingInfo.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> field.contains("position");
    }
}
