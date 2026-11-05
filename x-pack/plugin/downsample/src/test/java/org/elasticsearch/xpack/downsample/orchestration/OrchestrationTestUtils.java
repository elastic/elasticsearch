/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample.orchestration;

import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.downsample.DownsampleShardTaskParams;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.test.ESTestCase.between;
import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomLongBetween;
import static org.elasticsearch.test.ESTestCase.randomProjectIdOrDefault;
import static org.elasticsearch.test.ESTestCase.randomUUID;

final class OrchestrationTestUtils {

    private OrchestrationTestUtils() {}

    static DownsampleConfig randomConfig() {
        return new DownsampleConfig(
            randomFrom(DateHistogramInterval.HOUR, DateHistogramInterval.DAY),
            randomBoolean() ? null : randomFrom(DownsampleConfig.SamplingMethod.values())
        );
    }

    static DownsampleShardTaskParams randomParams(String targetIndex, ShardId sourceShardId) {
        long start = randomLongBetween(100_000, 200_000);
        return new DownsampleShardTaskParams(
            randomConfig(),
            targetIndex,
            start,
            start + randomLongBetween(1_000, 10_000),
            sourceShardId,
            new String[] { "metric_" + randomAlphaOfLength(3) },
            new String[] { "label_" + randomAlphaOfLength(3) },
            new String[] { "dimension_" + randomAlphaOfLength(3) },
            randomBoolean() ? Map.of() : Map.of("source_" + randomAlphaOfLength(3), "multi_" + randomAlphaOfLength(3))
        );
    }

    static ShardTaskDoc newShardTask(String targetIndex, String targetIndexUuid) {
        return newShardTask(randomProjectIdOrDefault(), targetIndex, targetIndexUuid);
    }

    static ShardTaskDoc newShardTask(ProjectId projectId, String targetIndex, String targetIndexUuid) {
        ShardId sourceShardId = new ShardId(
            new Index("source-" + randomAlphaOfLength(5).toLowerCase(Locale.ROOT), randomUUID()),
            between(0, 4)
        );
        return ShardTaskDoc.newTask(projectId, targetIndex, targetIndexUuid, randomParams(targetIndex, sourceShardId));
    }

    static IndexTaskDoc newIndexTask(String targetIndex) {
        return newIndexTask(randomProjectIdOrDefault(), targetIndex);
    }

    static IndexTaskDoc newIndexTask(ProjectId projectId, String targetIndex) {
        long now = randomLongBetween(1, 1_000_000);
        return IndexTaskDoc.newTask(
            projectId,
            targetIndex,
            "source-" + randomAlphaOfLength(5),
            randomUUID(),
            randomConfig(),
            now,
            now + 100_000
        );
    }

    static <T extends ToXContent> T roundTrip(T doc, CheckedFunction<XContentParser, T, IOException> parser) throws IOException {
        try (
            XContentParser xContentParser = XContentHelper.createParser(
                XContentParserConfiguration.EMPTY,
                XContentHelper.toXContent(doc, XContentType.JSON, false),
                XContentType.JSON
            )
        ) {
            return parser.apply(xContentParser);
        }
    }
}
