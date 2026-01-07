/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.apache.lucene.internal.hppc.IntArrayList;
import org.apache.lucene.internal.hppc.IntObjectHashMap;
import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.fielddata.HistogramValue;
import org.elasticsearch.index.fielddata.HistogramValues;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class TDigestHistogramFieldProducerTests extends ESTestCase {

    public void testLastValueProducer() throws IOException {
        var producer = TDigestHistogramFieldProducer.create("my-histogram", DownsampleConfig.SamplingMethod.LAST_VALUE);
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

    public void testAggregateProducer() throws IOException {
        var producer = TDigestHistogramFieldProducer.create("my-histogram", DownsampleConfig.SamplingMethod.AGGREGATE);
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

    HistogramValues createValuesInstance(IntArrayList docIdBuffer, HistogramValue[] values) {
        return new HistogramValues() {
            final IntObjectHashMap<HistogramValue> docIdToValue = IntObjectHashMap.from(docIdBuffer.toArray(), values);

            int currentDocId = -1;

            @Override
            public boolean advanceExact(int target) throws IOException {
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

}
