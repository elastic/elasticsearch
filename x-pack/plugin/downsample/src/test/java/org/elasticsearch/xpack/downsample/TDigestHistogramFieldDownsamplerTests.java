/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.apache.lucene.internal.hppc.IntArrayList;
import org.apache.lucene.internal.hppc.IntObjectHashMap;
import org.apache.lucene.search.Query;
import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.fielddata.HistogramValue;
import org.elasticsearch.index.fielddata.HistogramValues;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.TimeSeriesParams;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.aggregations.metrics.TDigestExecutionHint;
import org.elasticsearch.search.aggregations.metrics.TDigestState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.analytics.mapper.TDigestFieldMapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class TDigestHistogramFieldDownsamplerTests extends ESTestCase {

    public void testLastValueProducerForLegacyHistogram() throws IOException {
        var producer = TDigestHistogramFieldDownsampler.create(
            "my-histogram",
            createLegacyHistogramFieldType("my-histogram"),
            null,
            DownsampleConfig.SamplingMethod.LAST_VALUE
        );
        assertTrue(producer.isEmpty());
        assertEquals("my-histogram", producer.name());

        var docValues = createValuesInstance(
            IntArrayList.from(1, 2),
            new HistogramValue[] {
                histogram(new double[] { 1, 2 }, new long[] { 1, 2 }),
                histogram(new double[] { 1, 4 }, new long[] { 3, 4 }) }
        );
        producer.collect(docValues, IntArrayList.from(1, 2));
        assertFalse(producer.isEmpty());

        var builder = new XContentBuilder(XContentType.JSON.xContent(), new ByteArrayOutputStream());
        builder.startObject();
        producer.write(builder);
        builder.endObject();
        var content = Strings.toString(builder);
        assertThat(content, equalTo("{\"my-histogram\":{\"counts\":[1,2],\"values\":[1.0,2.0]}}"));
    }

    public void testAggregateProducerForLegacyHistogram() throws IOException {
        var producer = TDigestHistogramFieldDownsampler.create(
            "my-histogram",
            createLegacyHistogramFieldType("my-histogram"),
            null,
            DownsampleConfig.SamplingMethod.AGGREGATE
        );
        assertTrue(producer.isEmpty());
        assertEquals("my-histogram", producer.name());

        var docValues = createValuesInstance(
            IntArrayList.from(1, 2),
            new HistogramValue[] {
                histogram(new double[] { 1, 2 }, new long[] { 1, 2 }),
                histogram(new double[] { 1, 4 }, new long[] { 3, 4 }) }
        );
        producer.collect(docValues, IntArrayList.from(1, 2));
        assertFalse(producer.isEmpty());

        var builder = new XContentBuilder(XContentType.JSON.xContent(), new ByteArrayOutputStream());
        builder.startObject();
        producer.write(builder);
        builder.endObject();
        var content = Strings.toString(builder);
        assertThat(content, equalTo("{\"my-histogram\":{\"counts\":[4,2,4],\"values\":[1.0,2.0,4.0]}}"));
    }

    public void testLastValueProducerForTDigest() throws IOException {
        var producer = TDigestHistogramFieldDownsampler.create(
            "my-histogram",
            createTDigestFieldType(
                "my-histogram",
                randomBoolean() ? TDigestExecutionHint.DEFAULT : TDigestExecutionHint.HIGH_ACCURACY,
                randomDoubleBetween(1000, 10000, true)
            ),
            null,
            DownsampleConfig.SamplingMethod.LAST_VALUE
        );
        assertTrue(producer.isEmpty());
        assertEquals("my-histogram", producer.name());

        var docValues = createValuesInstance(
            IntArrayList.from(1, 2),
            new HistogramValue[] {
                histogram(new double[] { 1, 2 }, new long[] { 1, 2 }),
                histogram(new double[] { 1, 4 }, new long[] { 3, 4 }) }
        );
        producer.collect(docValues, IntArrayList.from(1, 2));
        assertFalse(producer.isEmpty());

        var builder = new XContentBuilder(XContentType.JSON.xContent(), new ByteArrayOutputStream());
        builder.startObject();
        producer.write(builder);
        builder.endObject();
        var content = Strings.toString(builder);
        assertThat(content, equalTo("{\"my-histogram\":{\"counts\":[1,2],\"centroids\":[1.0,2.0]}}"));
    }

    public void testAggregateProducerForTDigest() throws IOException {
        var hint = randomBoolean() ? TDigestExecutionHint.DEFAULT : TDigestExecutionHint.HIGH_ACCURACY;
        var compression = randomDoubleBetween(1000, 10000, true);
        var producer = TDigestHistogramFieldDownsampler.create(
            "my-histogram",
            createTDigestFieldType("my-histogram", hint, compression),
            null,
            DownsampleConfig.SamplingMethod.AGGREGATE
        );
        assertTrue(producer.isEmpty());
        assertEquals("my-histogram", producer.name());

        var docValues = createValuesInstance(
            IntArrayList.from(1, 2),
            new HistogramValue[] {
                histogram(new double[] { 1, 2 }, new long[] { 1, 2 }),
                histogram(new double[] { 1, 4 }, new long[] { 3, 4 }) }
        );
        producer.collect(docValues, IntArrayList.from(1, 2));
        assertFalse(producer.isEmpty());

        var builder = new XContentBuilder(XContentType.JSON.xContent(), new ByteArrayOutputStream());
        builder.startObject();
        producer.write(builder);
        builder.endObject();
        var content = Strings.toString(builder);
        assertThat(content, equalTo("{\"my-histogram\":{\"counts\":[4,2,4],\"centroids\":[1.0,2.0,4.0]}}"));
        assertThat(producer, instanceOf(TDigestHistogramFieldDownsampler.Aggregate.class));
        assertThat(
            ((TDigestHistogramFieldDownsampler.Aggregate) producer).getType(),
            equalTo(hint == TDigestExecutionHint.HIGH_ACCURACY ? TDigestState.Type.AVL_TREE : TDigestState.Type.MERGING)
        );
        assertThat(((TDigestHistogramFieldDownsampler.Aggregate) producer).getCompression(), equalTo(compression));
    }

    HistogramValues createValuesInstance(IntArrayList docIdBuffer, HistogramValue[] values) {
        return new HistogramValues() {
            final IntObjectHashMap<HistogramValue> docIdToValue = IntObjectHashMap.from(docIdBuffer.toArray(), values);

            int currentDocId = -1;

            @Override
            public boolean advanceExact(int target) {
                currentDocId = target;
                return docIdToValue.containsKey(target);
            }

            @Override
            public HistogramValue histogram() {
                return docIdToValue.get(currentDocId);
            }
        };
    }

    private HistogramValue histogram(double[] value, long[] count) {
        return new HistogramValue() {
            int i = -1;

            @Override
            public boolean next() {
                i++;
                return i < value.length;
            }

            @Override
            public double value() {
                return value[i];
            }

            @Override
            public long count() {
                return count[i];
            }
        };
    }

    private MappedFieldType createLegacyHistogramFieldType(String fieldName) {
        return new MappedFieldType(fieldName, null, false, Map.of()) {
            @Override
            public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
                return null;
            }

            @Override
            public String typeName() {
                return "histogram";
            }

            @Override
            public Query termQuery(Object value, SearchExecutionContext context) {
                return null;
            }
        };
    }

    private MappedFieldType createTDigestFieldType(String fieldName, TDigestExecutionHint executionHint, Double compression) {
        return new TDigestFieldMapper.TDigestFieldType(
            fieldName,
            Map.of(),
            randomBoolean() ? null : TimeSeriesParams.MetricType.HISTOGRAM,
            executionHint,
            compression
        );
    }

}
