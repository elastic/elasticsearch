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

package org.elasticsearch.snapshots;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.List;

public class SnapshotFeatureInfoTests extends AbstractSerializingTestCase<SnapshotFeatureInfo> {
    @Override
    protected SnapshotFeatureInfo doParseInstance(XContentParser parser) throws IOException {
        return SnapshotFeatureInfo.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<SnapshotFeatureInfo> instanceReader() {
        return SnapshotFeatureInfo::new;
    }

    @Override
    protected SnapshotFeatureInfo createTestInstance() {
        String feature = randomAlphaOfLengthBetween(5,20);
        List<String> indices = randomList(1, 10, () -> randomAlphaOfLengthBetween(5, 20));
        return new SnapshotFeatureInfo(feature, indices);
    }

    @Override
    protected SnapshotFeatureInfo mutateInstance(SnapshotFeatureInfo instance) throws IOException {
        if (randomBoolean()) {
            return new SnapshotFeatureInfo(randomValueOtherThan(instance.getPluginName(), () -> randomAlphaOfLengthBetween(5, 20)),
                instance.getIndices());
        } else {
            return new SnapshotFeatureInfo(instance.getPluginName(),
                randomList(1, 10, () -> randomValueOtherThanMany(instance.getIndices()::contains,
                    () -> randomAlphaOfLengthBetween(5, 20))));
        }
    }
}
