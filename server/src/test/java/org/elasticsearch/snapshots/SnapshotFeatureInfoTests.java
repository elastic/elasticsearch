/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

public class SnapshotFeatureInfoTests extends AbstractXContentSerializingTestCase<SnapshotFeatureInfo> {
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
        return randomSnapshotFeatureInfo();
    }

    public static SnapshotFeatureInfo randomSnapshotFeatureInfo() {
        String feature = randomAlphaOfLengthBetween(5, 20);
        List<String> indices = randomList(1, 10, () -> randomAlphaOfLengthBetween(5, 20));
        return new SnapshotFeatureInfo(feature, indices);
    }

    @Override
    protected SnapshotFeatureInfo mutateInstance(SnapshotFeatureInfo instance) {
        if (randomBoolean()) {
            return new SnapshotFeatureInfo(
                randomValueOtherThan(instance.getPluginName(), () -> randomAlphaOfLengthBetween(5, 20)),
                instance.getIndices()
            );
        } else {
            return new SnapshotFeatureInfo(
                instance.getPluginName(),
                randomList(1, 10, () -> randomValueOtherThanMany(instance.getIndices()::contains, () -> randomAlphaOfLengthBetween(5, 20)))
            );
        }
    }
}
