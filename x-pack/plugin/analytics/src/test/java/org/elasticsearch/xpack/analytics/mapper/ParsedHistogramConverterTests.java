/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.mapper;

import io.opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint;

import org.elasticsearch.common.Strings;
import org.elasticsearch.exponentialhistogram.BucketIterator;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramCircuitBreaker;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramMerger;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramTestUtils;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent;
import org.elasticsearch.exponentialhistogram.ExponentialScaleUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.DataPoint;
import org.elasticsearch.xpack.oteldata.otlp.docbuilder.MappingHints;

import java.io.IOException;
import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

public class ParsedHistogramConverterTests extends ESTestCase {

    public void testExponentialHistogramRoundTrip() {
        ExponentialHistogram input = ExponentialHistogramTestUtils.randomHistogram();
        HistogramParser.ParsedHistogram tdigest = ParsedHistogramConverter.exponentialToTDigest(toParsed(input));
        ExponentialHistogramParser.ParsedExponentialHistogram output = ParsedHistogramConverter.tDigestToExponential(tdigest);

        // the conversion looses the width of the original buckets, but the bucket centers (arithmetic mean of boundaries)
        // should be very close

        assertThat(output.zeroCount(), equalTo(input.zeroBucket().count()));
        assertArithmeticBucketCentersClose(input.negativeBuckets().iterator(), output.negativeBuckets(), output.scale());
        assertArithmeticBucketCentersClose(input.positiveBuckets().iterator(), output.positiveBuckets(), output.scale());
    }

    private static void assertArithmeticBucketCentersClose(
        BucketIterator originalBuckets,
        List<IndexWithCount> convertedBuckets,
        int convertedScale
    ) {
        for (IndexWithCount convertedBucket : convertedBuckets) {
            assertThat(originalBuckets.hasNext(), equalTo(true));

            double originalCenter = (ExponentialScaleUtils.getLowerBucketBoundary(originalBuckets.peekIndex(), originalBuckets.scale())
                + ExponentialScaleUtils.getUpperBucketBoundary(originalBuckets.peekIndex(), originalBuckets.scale())) / 2.0;
            double convertedCenter = (ExponentialScaleUtils.getLowerBucketBoundary(convertedBucket.index(), convertedScale)
                + ExponentialScaleUtils.getUpperBucketBoundary(convertedBucket.index(), convertedScale)) / 2.0;

            double relativeError = Math.abs(convertedCenter - originalCenter) / Math.abs(originalCenter);
            assertThat(
                "original center=" + originalCenter + ", converted center=" + convertedCenter + ", relative error=" + relativeError,
                relativeError,
                closeTo(0, 0.0000001)
            );

            originalBuckets.advance();
        }
        assertThat(originalBuckets.hasNext(), equalTo(false));
    }

    public void testToExponentialHistogramConversionWithCloseCentroids() {
        // build a t-digest with two centroids very close to each other
        List<Double> centroids = List.of(1.0, Math.nextAfter(1.0, 2));
        List<Long> counts = List.of(1L, 2L);

        HistogramParser.ParsedHistogram input = new HistogramParser.ParsedHistogram(centroids, counts);
        ExponentialHistogramParser.ParsedExponentialHistogram converted = ParsedHistogramConverter.tDigestToExponential(input);

        assertThat(converted.zeroCount(), equalTo(0L));
        List<IndexWithCount> posBuckets = converted.positiveBuckets();
        assertThat(posBuckets.size(), equalTo(2));
        assertThat(posBuckets.get(0).index(), lessThan(posBuckets.get(1).index()));
        assertThat(posBuckets.get(0).count(), equalTo(1L));
        assertThat(posBuckets.get(1).count(), equalTo(2L));
    }

    public void testToExponentialHistogramConversionWithZeroCounts() {
        // build a t-digest with two centroids very close to each other
        List<Double> centroids = List.of(1.0, 2.0, 3.0);
        List<Long> counts = List.of(1L, 0L, 2L);

        HistogramParser.ParsedHistogram input = new HistogramParser.ParsedHistogram(centroids, counts);
        ExponentialHistogramParser.ParsedExponentialHistogram converted = ParsedHistogramConverter.tDigestToExponential(input);

        assertThat(converted.zeroCount(), equalTo(0L));
        List<IndexWithCount> posBuckets = converted.positiveBuckets();
        assertThat(posBuckets.size(), equalTo(2));
        assertThat(posBuckets.get(0).index(), lessThan(posBuckets.get(1).index()));
        assertThat(posBuckets.get(0).count(), equalTo(1L));
        assertThat(posBuckets.get(1).count(), equalTo(2L));
    }

    public void testToTDigestConversionMergesCentroids() {
        // build a histogram with two buckets very close to zero
        ExponentialHistogram input = ExponentialHistogram.builder(ExponentialHistogram.MAX_SCALE, ExponentialHistogramCircuitBreaker.noop())
            .setPositiveBucket(ExponentialHistogram.MIN_INDEX, 1)
            .setPositiveBucket(ExponentialHistogram.MIN_INDEX + 1, 2)
            .build();
        // due to rounding errors they end up as the same centroid, but should have the count merged
        HistogramParser.ParsedHistogram converted = ParsedHistogramConverter.exponentialToTDigest(toParsed(input));
        assertThat(converted.values(), equalTo(List.of(0.0)));
        assertThat(converted.counts(), equalTo(List.of(3L)));
    }

    public void testSameConversionBehaviourAsOtlpMetricsEndpoint() {
        // our histograms are sparse, opentelemetry ones are dense.
        // to test against the OTLP conversion algorithm, we need to make our random histogram dense enough first
        ExponentialHistogram input = makeDense(ExponentialHistogramTestUtils.randomHistogram());
        DataPoint.ExponentialHistogram otelDataPoint = toOtelProtoDataPoint(input);

        ExponentialHistogramParser.ParsedExponentialHistogram parsedExponential = toParsed(input);
        HistogramParser.ParsedHistogram convertedViaOtlpEndpoint = toParsed(otelDataPoint);

        HistogramParser.ParsedHistogram convertedViaMapper = ParsedHistogramConverter.exponentialToTDigest(parsedExponential);

        assertThat(convertedViaMapper.counts(), equalTo(convertedViaOtlpEndpoint.counts()));
        assertThat(convertedViaMapper.values().size(), equalTo(convertedViaOtlpEndpoint.values().size()));
        for (int i = 0; i < convertedViaMapper.values().size(); i++) {
            double actual = convertedViaOtlpEndpoint.values().get(i);
            double expected = convertedViaMapper.values().get(i);
            if (actual != expected) {
                double relativeError = Math.abs(actual - expected) / Math.abs(actual);
                assertThat(
                    "centroid " + i + ": actual=" + actual + " expected=" + expected + ", relative error=" + relativeError,
                    relativeError,
                    closeTo(0, 0.000001)
                );
            }
        }
    }

    private ExponentialHistogram makeDense(ExponentialHistogram histo) {
        ExponentialHistogram result = histo;
        while (getIndexRange(result) > 10_000 || hasNonIntegerIndices(result)) {
            int numBuckets = histo.negativeBuckets().bucketCount() + histo.positiveBuckets().bucketCount();
            ExponentialHistogramMerger merger = ExponentialHistogramMerger.createWithMaxScale(
                Math.max(4, numBuckets),
                result.scale() - 1,
                ExponentialHistogramCircuitBreaker.noop()
            );
            merger.add(result);
            result = merger.getAndClear();
        }
        return result;
    }

    private boolean hasNonIntegerIndices(ExponentialHistogram result) {
        return LongStream.concat(result.positiveBuckets().maxBucketIndex().stream(), result.negativeBuckets().maxBucketIndex().stream())
            .anyMatch(maxIndex -> maxIndex > Integer.MAX_VALUE);
    }

    private long getIndexRange(ExponentialHistogram histo) {
        long range = 0;
        BucketIterator neg = histo.negativeBuckets().iterator();
        if (neg.hasNext()) {
            range += histo.negativeBuckets().maxBucketIndex().getAsLong() - neg.peekIndex() + 1;
        }
        BucketIterator pos = histo.positiveBuckets().iterator();
        if (pos.hasNext()) {
            range += histo.positiveBuckets().maxBucketIndex().getAsLong() - pos.peekIndex() + 1;
        }
        return range;
    }

    private DataPoint.ExponentialHistogram toOtelProtoDataPoint(ExponentialHistogram input) {
        ExponentialHistogramDataPoint protoPoint = ExponentialHistogramDataPoint.newBuilder()
            .setScale(input.scale())
            .setNegative(toOtelProtoBuckets(input.negativeBuckets()))
            .setPositive(toOtelProtoBuckets(input.positiveBuckets()))
            .setZeroCount(input.zeroBucket().count())
            .setZeroThreshold(input.zeroBucket().zeroThreshold())
            .build();
        return new DataPoint.ExponentialHistogram(protoPoint, null);
    }

    private ExponentialHistogramDataPoint.Buckets toOtelProtoBuckets(ExponentialHistogram.Buckets buckets) {
        ExponentialHistogramDataPoint.Buckets.Builder builder = ExponentialHistogramDataPoint.Buckets.newBuilder();
        if (buckets.bucketCount() == 0) {
            return builder.build();
        }
        BucketIterator it = buckets.iterator();
        int offset = (int) it.peekIndex();
        builder.setOffset(offset);
        int denseBucketCount = (int) (buckets.maxBucketIndex().getAsLong() - offset + 1);
        for (int i = 0; i < denseBucketCount; i++) {
            if (it.peekIndex() == (offset + i)) {
                builder.addBucketCounts(it.peekCount());
                it.advance();
            } else {
                builder.addBucketCounts(0L);
            }
        }
        return builder.build();
    }

    private HistogramParser.ParsedHistogram toParsed(DataPoint.ExponentialHistogram point) {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            point.buildMetricValue(MappingHints.empty(), builder);
            String json = Strings.toString(builder);
            try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
                parser.nextToken();
                parser.nextToken(); // skip START_OBJECT token
                return HistogramParser.parse("testing", parser);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private ExponentialHistogramParser.ParsedExponentialHistogram toParsed(ExponentialHistogram histo) {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            ExponentialHistogramXContent.serialize(builder, histo);
            String json = Strings.toString(builder);
            try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
                parser.nextToken();
                parser.nextToken(); // skip START_OBJECT token
                return ExponentialHistogramParser.parse("testing", parser);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
