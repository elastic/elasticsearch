/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform.transforms;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;

public class TransformStatsTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        xContentTester(this::createParser,
            TransformStatsTests::randomInstance,
            TransformStatsTests::toXContent,
            TransformStats::fromXContent)
                .supportsUnknownFields(true)
                .randomFieldsExcludeFilter(field -> field.equals("node.attributes") || field.contains("position"))
                .test();
    }

    public static TransformStats randomInstance() {
        return new TransformStats(randomAlphaOfLength(10),
            randomBoolean() ? null : randomFrom(TransformStats.State.values()),
            randomBoolean() ? null : randomAlphaOfLength(100),
            randomBoolean() ? null : NodeAttributesTests.createRandom(),
            TransformIndexerStatsTests.randomStats(),
            randomBoolean() ? null : TransformCheckpointingInfoTests.randomTransformCheckpointingInfo());
    }

    public static void toXContent(TransformStats stats, XContentBuilder builder) throws IOException {
        builder.startObject();
        builder.field(TransformStats.ID.getPreferredName(), stats.getId());
        if (stats.getState() != null) {
            builder.field(TransformStats.STATE_FIELD.getPreferredName(),
                stats.getState().value());
        }
        if (stats.getReason() != null) {
            builder.field(TransformStats.REASON_FIELD.getPreferredName(), stats.getReason());
        }
        if (stats.getNode() != null) {
            builder.field(TransformStats.NODE_FIELD.getPreferredName());
            stats.getNode().toXContent(builder, ToXContent.EMPTY_PARAMS);
        }
        builder.field(TransformStats.STATS_FIELD.getPreferredName());
        TransformIndexerStatsTests.toXContent(stats.getIndexerStats(), builder);
        if (stats.getCheckpointingInfo() != null) {
            builder.field(TransformStats.CHECKPOINTING_INFO_FIELD.getPreferredName());
            TransformCheckpointingInfoTests.toXContent(stats.getCheckpointingInfo(), builder);
        }
        builder.endObject();
    }
}
