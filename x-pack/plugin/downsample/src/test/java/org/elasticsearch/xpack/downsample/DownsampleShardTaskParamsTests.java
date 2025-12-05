/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DownsampleShardTaskParamsTests extends AbstractXContentSerializingTestCase<DownsampleShardTaskParams> {
    @Override
    protected Writeable.Reader<DownsampleShardTaskParams> instanceReader() {
        return DownsampleShardTaskParams::new;
    }

    @Override
    protected DownsampleShardTaskParams createTestInstance() {
        long startTime = randomLongBetween(100000, 200000);
        long endTime = startTime + randomLongBetween(1000, 10_000);
        String[] dimensions = randomBoolean() ? generateRandomStringArray(5, 5, false, true) : new String[] {};
        String[] labels = generateRandomStringArray(5, 5, false, false);
        return new DownsampleShardTaskParams(
            new DownsampleConfig(randomFrom(DateHistogramInterval.HOUR, DateHistogramInterval.DAY)),
            randomAlphaOfLength(5),
            startTime,
            endTime,
            new ShardId(new Index(randomAlphaOfLength(5), "n/a"), between(0, 5)),
            generateRandomStringArray(5, 5, false, false),
            labels,
            dimensions,
            generateRandomMap(labels, dimensions)
        );
    }

    @Override
    protected DownsampleShardTaskParams mutateInstance(DownsampleShardTaskParams in) throws IOException {
        return switch (between(0, 8)) {
            case 0 -> new DownsampleShardTaskParams(
                new DownsampleConfig(randomFrom(DateHistogramInterval.WEEK, DateHistogramInterval.MONTH)),
                in.downsampleIndex(),
                in.indexStartTimeMillis(),
                in.indexEndTimeMillis(),
                in.shardId(),
                in.metrics(),
                in.labels(),
                in.dimensions(),
                in.multiFieldSources()
            );
            case 1 -> new DownsampleShardTaskParams(
                in.downsampleConfig(),
                randomAlphaOfLength(6),
                in.indexStartTimeMillis(),
                in.indexEndTimeMillis(),
                in.shardId(),
                in.metrics(),
                in.labels(),
                in.dimensions(),
                in.multiFieldSources()
            );
            case 2 -> new DownsampleShardTaskParams(
                in.downsampleConfig(),
                in.downsampleIndex(),
                in.indexStartTimeMillis() + between(1, 100),
                in.indexEndTimeMillis() + between(1, 100),
                in.shardId(),
                in.metrics(),
                in.labels(),
                in.dimensions(),
                in.multiFieldSources()
            );
            case 3 -> new DownsampleShardTaskParams(
                in.downsampleConfig(),
                in.downsampleIndex(),
                in.indexStartTimeMillis(),
                in.indexEndTimeMillis() + between(10, 100),
                new ShardId(new Index(randomAlphaOfLength(6), "n/a"), between(0, 5)),
                in.metrics(),
                in.labels(),
                in.dimensions(),
                in.multiFieldSources()
            );
            case 4 -> new DownsampleShardTaskParams(
                in.downsampleConfig(),
                in.downsampleIndex(),
                in.indexStartTimeMillis(),
                in.indexEndTimeMillis() + between(10, 100),
                in.shardId(),
                in.metrics(),
                in.labels(),
                in.dimensions(),
                in.multiFieldSources()
            );
            case 5 -> new DownsampleShardTaskParams(
                in.downsampleConfig(),
                in.downsampleIndex(),
                in.indexStartTimeMillis(),
                in.indexEndTimeMillis(),
                in.shardId(),
                generateRandomStringArray(6, 6, false, false),
                in.labels(),
                in.dimensions(),
                in.multiFieldSources()
            );
            case 6 -> new DownsampleShardTaskParams(
                in.downsampleConfig(),
                in.downsampleIndex(),
                in.indexStartTimeMillis(),
                in.indexEndTimeMillis(),
                in.shardId(),
                in.metrics(),
                generateRandomStringArray(6, 6, false, false),
                in.dimensions(),
                in.multiFieldSources()
            );
            case 7 -> new DownsampleShardTaskParams(
                in.downsampleConfig(),
                in.downsampleIndex(),
                in.indexStartTimeMillis(),
                in.indexEndTimeMillis(),
                in.shardId(),
                in.metrics(),
                in.labels(),
                generateRandomStringArray(6, 6, false, false),
                in.multiFieldSources()
            );
            case 8 -> new DownsampleShardTaskParams(
                in.downsampleConfig(),
                in.downsampleIndex(),
                in.indexStartTimeMillis(),
                in.indexEndTimeMillis(),
                in.shardId(),
                in.metrics(),
                in.labels(),
                in.dimensions(),
                randomValueOtherThan(in.multiFieldSources(), () -> generateRandomMap(in.labels(), in.dimensions()))
            );
            default -> throw new AssertionError("unknown option");
        };
    }

    private Map<String, String> generateRandomMap(String[] labels, String[] dimensions) {
        int labelSize = randomIntBetween(0, labels.length);
        int dimensionSize = randomIntBetween(0, dimensions.length);
        Map<String, String> map = new HashMap<>(labelSize + dimensionSize);
        for (int i = 0; i < labelSize; i++) {
            map.put(labels[i], labels[i] + "." + randomAlphaOfLength(10));
        }
        for (int i = 0; i < dimensionSize; i++) {
            map.put(dimensions[i], dimensions[i] + "." + randomAlphaOfLength(10));
        }
        return map;
    }

    @Override
    protected DownsampleShardTaskParams doParseInstance(XContentParser parser) throws IOException {
        return DownsampleShardTaskParams.fromXContent(parser);
    }
}
