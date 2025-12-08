/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.datapoint;

import io.opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint.Buckets;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.List;

import static io.opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint.newBuilder;
import static org.hamcrest.Matchers.equalTo;

/**
 * @see <a href="https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/v0.132.0/exporter/elasticsearchexporter/internal/exphistogram/exphistogram_test.go">
 *     OpenTelemetry Collector Exponential Histogram Tests
 * </a>
 */
public class ExponentialHistogramToExponentialHistogramConverterTests extends ESTestCase {

    private final ExponentialHistogramDataPoint dataPoint;
    private final String expectedJson;

    public ExponentialHistogramToExponentialHistogramConverterTests(
        String name,
        ExponentialHistogramDataPoint.Builder builder,
        String expectedJson
    ) throws IOException {
        this.dataPoint = builder.build();
        this.expectedJson = expectedJson;
    }

    public void testExponentialHistograms() throws IOException {
        try (var builder = JsonXContent.contentBuilder()) {
            ExponentialHistogramConverter.buildExponentialHistogram(dataPoint, builder);
            String json = Strings.toString(builder);
            assertThat(json, equalTo(expectedJson));
        }
    }

    @ParametersFactory(argumentFormatting = "%1$s")
    public static List<Object[]> testCases() {
        return List.of(
            new Object[] { "empty", newBuilder(), "{\"scale\":0}" },
            new Object[] { "empty, scale=1", newBuilder().setScale(1), "{\"scale\":1}" },
            new Object[] { "empty, scale=-1", newBuilder().setScale(-1), "{\"scale\":-1}" },
            new Object[] { "zeros", newBuilder().setZeroCount(1), "{\"scale\":0,\"zero\":{\"count\":1}}" },
            new Object[] {
                "scale=0",
                newBuilder().setZeroCount(1)
                    .setPositive(Buckets.newBuilder().setOffset(0).addAllBucketCounts(List.of(1L, 1L)))
                    .setNegative(Buckets.newBuilder().setOffset(0).addAllBucketCounts(List.of(1L, 1L))),
                "{\"scale\":0,\"zero\":{\"count\":1},"
                    + "\"negative\":{\"indices\":[0,1],\"counts\":[1,1]},\"positive\":{\"indices\":[0,1],\"counts\":[1,1]}}" },
            new Object[] {
                "scale=0, no zeros",
                newBuilder().setPositive(Buckets.newBuilder().setOffset(0).addAllBucketCounts(List.of(1L, 1L)))
                    .setNegative(Buckets.newBuilder().setOffset(0).addAllBucketCounts(List.of(1L, 1L))),
                "{\"scale\":0,\"negative\":{\"indices\":[0,1],\"counts\":[1,1]},\"positive\":{\"indices\":[0,1],\"counts\":[1,1]}}" },
            new Object[] {
                "scale=0, offset=1",
                newBuilder().setZeroCount(1)
                    .setPositive(Buckets.newBuilder().setOffset(1).addAllBucketCounts(List.of(1L, 1L)))
                    .setNegative(Buckets.newBuilder().setOffset(1).addAllBucketCounts(List.of(1L, 1L))),
                "{\"scale\":0,\"zero\":{\"count\":1},"
                    + "\"negative\":{\"indices\":[1,2],\"counts\":[1,1]},\"positive\":{\"indices\":[1,2],\"counts\":[1,1]}}" },
            new Object[] {
                "scale=0, offset=-1",
                newBuilder().setZeroCount(1)
                    .setPositive(Buckets.newBuilder().setOffset(-1).addAllBucketCounts(List.of(1L, 1L)))
                    .setNegative(Buckets.newBuilder().setOffset(-1).addAllBucketCounts(List.of(1L, 1L))),
                "{\"scale\":0,\"zero\":{\"count\":1},"
                    + "\"negative\":{\"indices\":[-1,0],\"counts\":[1,1]},\"positive\":{\"indices\":[-1,0],\"counts\":[1,1]}}" },
            new Object[] {
                "scale=0, different offsets",
                newBuilder().setZeroCount(1)
                    .setPositive(Buckets.newBuilder().setOffset(-1).addAllBucketCounts(List.of(1L, 1L)))
                    .setNegative(Buckets.newBuilder().setOffset(1).addAllBucketCounts(List.of(1L, 1L))),
                "{\"scale\":0,\"zero\":{\"count\":1},"
                    + "\"negative\":{\"indices\":[1,2],\"counts\":[1,1]},\"positive\":{\"indices\":[-1,0],\"counts\":[1,1]}}" },
            new Object[] {
                "scale=-1",
                newBuilder().setScale(-1)
                    .setZeroCount(1)
                    .setPositive(Buckets.newBuilder().setOffset(0).addAllBucketCounts(List.of(1L, 1L)))
                    .setNegative(Buckets.newBuilder().setOffset(0).addAllBucketCounts(List.of(1L, 1L))),
                "{\"scale\":-1,\"zero\":{\"count\":1},"
                    + "\"negative\":{\"indices\":[0,1],\"counts\":[1,1]},\"positive\":{\"indices\":[0,1],\"counts\":[1,1]}}" },
            new Object[] {
                "scale=1",
                newBuilder().setScale(1)
                    .setZeroCount(1)
                    .setPositive(Buckets.newBuilder().setOffset(0).addAllBucketCounts(List.of(1L, 1L)))
                    .setNegative(Buckets.newBuilder().setOffset(0).addAllBucketCounts(List.of(1L, 1L))),
                "{\"scale\":1,\"zero\":{\"count\":1},"
                    + "\"negative\":{\"indices\":[0,1],\"counts\":[1,1]},\"positive\":{\"indices\":[0,1],\"counts\":[1,1]}}" },
            new Object[] {
                "zero count and threshold",
                newBuilder().setZeroCount(2).setZeroThreshold(5.5),
                "{\"scale\":0,\"zero\":{\"count\":2,\"threshold\":5.5}}" },
            new Object[] {
                "zero count and threshold with buckets",
                newBuilder().setZeroCount(3)
                    .setZeroThreshold(2.2)
                    .setNegative(Buckets.newBuilder().setOffset(-2).addAllBucketCounts(List.of(0L, 4L, 0L, 5L)))
                    .setPositive(Buckets.newBuilder().setOffset(1).addAllBucketCounts(List.of(0L, 0L, 6L, 0L))),
                "{\"scale\":0,\"zero\":{\"count\":3,\"threshold\":2.2},"
                    + "\"negative\":{\"indices\":[-1,1],\"counts\":[4,5]},\"positive\":{\"indices\":[3],\"counts\":[6]}}" },
            new Object[] {
                "omit zero bucket counts",
                newBuilder().setNegative(Buckets.newBuilder().setOffset(-2).addAllBucketCounts(List.of(0L, 0L, 7L, 0L)))
                    .setPositive(Buckets.newBuilder().setOffset(0).addAllBucketCounts(List.of(0L, 8L, 0L))),
                "{\"scale\":0,\"negative\":{\"indices\":[0],\"counts\":[7]},\"positive\":{\"indices\":[1],\"counts\":[8]}}" },
            new Object[] {
                "sum only",
                newBuilder().setSum(123.45).setPositive(Buckets.newBuilder().setOffset(0).addAllBucketCounts(List.of(7L))),
                "{\"scale\":0,\"positive\":{\"indices\":[0],\"counts\":[7]},\"sum\":123.45}" },
            new Object[] {
                "min only",
                newBuilder().setMin(-10.5).setNegative(Buckets.newBuilder().setOffset(-2).addAllBucketCounts(List.of(0L, 5L, 0L))),
                "{\"scale\":0,\"negative\":{\"indices\":[-1],\"counts\":[5]},\"min\":-10.5}" },
            new Object[] {
                "max only",
                newBuilder().setMax(99.9).setPositive(Buckets.newBuilder().setOffset(2).addAllBucketCounts(List.of(0L, 0L, 8L))),
                "{\"scale\":0,\"positive\":{\"indices\":[4],\"counts\":[8]},\"max\":99.9}" },
            new Object[] {
                "sum and min",
                newBuilder().setSum(10.0)
                    .setMin(-5.0)
                    .setNegative(Buckets.newBuilder().setOffset(-1).addAllBucketCounts(List.of(0L, 3L)))
                    .setPositive(Buckets.newBuilder().setOffset(0).addAllBucketCounts(List.of(2L))),
                "{\"scale\":0,"
                    + "\"negative\":{\"indices\":[0],\"counts\":[3]},"
                    + "\"positive\":{\"indices\":[0],\"counts\":[2]},\"sum\":10.0,\"min\":-5.0}" },
            new Object[] {
                "sum, min, and max",
                newBuilder().setSum(100.0)
                    .setMin(-50.0)
                    .setMax(50.0)
                    .setNegative(Buckets.newBuilder().setOffset(-2).addAllBucketCounts(List.of(0L, 4L)))
                    .setPositive(Buckets.newBuilder().setOffset(1).addAllBucketCounts(List.of(5L, 0L))),
                "{\"scale\":0,"
                    + "\"negative\":{\"indices\":[-1],\"counts\":[4]},"
                    + "\"positive\":{\"indices\":[1],\"counts\":[5]},\"sum\":100.0,\"min\":-50.0,\"max\":50.0}" },
            new Object[] {
                "sum, min, max, zero, buckets",
                newBuilder().setSum(200.0)
                    .setMin(-100.0)
                    .setMax(100.0)
                    .setZeroCount(2)
                    .setZeroThreshold(1.5)
                    .setNegative(Buckets.newBuilder().setOffset(-1).addAllBucketCounts(List.of(0L, 3L, 0L)))
                    .setPositive(Buckets.newBuilder().setOffset(0).addAllBucketCounts(List.of(4L, 0L, 0L))),
                "{\"scale\":0,\"zero\":{\"count\":2,\"threshold\":1.5},"
                    + "\"negative\":{\"indices\":[0],\"counts\":[3]},"
                    + "\"positive\":{\"indices\":[0],\"counts\":[4]},\"sum\":200.0,\"min\":-100.0,\"max\":100.0}" },
            new Object[] {
                "only positive buckets",
                newBuilder().setPositive(Buckets.newBuilder().setOffset(2).addAllBucketCounts(List.of(0L, 0L, 9L, 0L))),
                "{\"scale\":0,\"positive\":{\"indices\":[4],\"counts\":[9]}}" },
            new Object[] {
                "only negative buckets",
                newBuilder().setNegative(Buckets.newBuilder().setOffset(-3).addAllBucketCounts(List.of(0L, 0L, 0L, 6L))),
                "{\"scale\":0,\"negative\":{\"indices\":[0],\"counts\":[6]}}" }
        );
    }
}
