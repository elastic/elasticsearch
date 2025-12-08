/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.apache.lucene.internal.hppc.IntArrayList;
import org.apache.lucene.internal.hppc.IntObjectHashMap;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.fielddata.FormattedDocValues;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class LastValueFieldProducerTests extends AggregatorTestCase {

    public void testLastValueKeyword() throws IOException {
        LastValueFieldProducer lastValueFieldProducer = new LastValueFieldProducer(randomAlphanumericOfLength(10), randomBoolean());
        assertThat(lastValueFieldProducer.lastValue(), nullValue());
        var docIdBuffer = IntArrayList.from(0, 1, 2);
        var values = createValuesInstance(docIdBuffer, new String[] { "aaa", "bbb", "ccc" });
        lastValueFieldProducer.collect(values, docIdBuffer);
        assertThat(lastValueFieldProducer.lastValue(), equalTo("aaa"));
        lastValueFieldProducer.reset();
        assertThat(lastValueFieldProducer.lastValue(), nullValue());
    }

    public void testLastValueDouble() throws IOException {
        LastValueFieldProducer lastValueFieldProducer = new LastValueFieldProducer(randomAlphanumericOfLength(10), randomBoolean());
        assertThat(lastValueFieldProducer.lastValue(), nullValue());
        var docIdBuffer = IntArrayList.from(0, 1, 2);
        var values = createValuesInstance(docIdBuffer, new Double[] { 10.20D, 17.30D, 12.60D });
        lastValueFieldProducer.collect(values, docIdBuffer);
        assertThat(lastValueFieldProducer.lastValue(), equalTo(10.20D));
        lastValueFieldProducer.reset();
        assertThat(lastValueFieldProducer.lastValue(), nullValue());
    }

    public void testLastValueInteger() throws IOException {
        LastValueFieldProducer lastValueFieldProducer = new LastValueFieldProducer(randomAlphanumericOfLength(10), randomBoolean());
        assertThat(lastValueFieldProducer.lastValue(), nullValue());
        var docIdBuffer = IntArrayList.from(0, 1, 2);
        var values = createValuesInstance(docIdBuffer, new Integer[] { 10, 17, 12 });
        lastValueFieldProducer.collect(values, docIdBuffer);
        assertThat(lastValueFieldProducer.lastValue(), equalTo(10));
        lastValueFieldProducer.reset();
        assertThat(lastValueFieldProducer.lastValue(), nullValue());
    }

    public void testLastValueLong() throws IOException {
        LastValueFieldProducer lastValueFieldProducer = new LastValueFieldProducer(randomAlphanumericOfLength(10), randomBoolean());
        assertThat(lastValueFieldProducer.lastValue(), nullValue());
        var docIdBuffer = IntArrayList.from(0, 1, 2);
        var values = createValuesInstance(docIdBuffer, new Long[] { 10L, 17L, 12L });
        lastValueFieldProducer.collect(values, docIdBuffer);
        assertThat(lastValueFieldProducer.lastValue(), equalTo(10L));
        lastValueFieldProducer.reset();
        assertThat(lastValueFieldProducer.lastValue(), nullValue());
    }

    public void testLastValueBoolean() throws IOException {
        LastValueFieldProducer lastValueFieldProducer = new LastValueFieldProducer(randomAlphanumericOfLength(10), randomBoolean());
        assertThat(lastValueFieldProducer.lastValue(), nullValue());
        var docIdBuffer = IntArrayList.from(0, 1, 2);
        var values = createValuesInstance(docIdBuffer, new Boolean[] { true, false, false });
        lastValueFieldProducer.collect(values, docIdBuffer);
        assertThat(lastValueFieldProducer.lastValue(), equalTo(true));
        lastValueFieldProducer.reset();
        assertThat(lastValueFieldProducer.lastValue(), nullValue());
    }

    public void testLastValueMultiValue() throws IOException {
        var docIdBuffer = IntArrayList.from(0);
        Boolean[] multiValue = new Boolean[] { true, false };
        var values = new FormattedDocValues() {

            Iterator<Boolean> iterator = Arrays.stream(multiValue).iterator();

            @Override
            public boolean advanceExact(int docId) {
                return true;
            }

            @Override
            public int docValueCount() {
                return 2;
            }

            @Override
            public Object nextValue() {
                return iterator.next();
            }
        };

        LastValueFieldProducer singleLastValueProducer = new LastValueFieldProducer(randomAlphanumericOfLength(10), false);
        assertThat(singleLastValueProducer.lastValue(), nullValue());
        singleLastValueProducer.collect(values, docIdBuffer);
        assertThat(singleLastValueProducer.lastValue(), equalTo(true));
        // Ensure we did not read all the available values
        assertThat(values.iterator.hasNext(), equalTo(true));
        singleLastValueProducer.reset();
        assertThat(singleLastValueProducer.lastValue(), nullValue());

        values.iterator = Arrays.stream(multiValue).iterator();
        LastValueFieldProducer multiLastValueProducer = new LastValueFieldProducer(randomAlphanumericOfLength(10), true);
        assertThat(multiLastValueProducer.lastValue(), nullValue());
        multiLastValueProducer.collect(values, docIdBuffer);
        assertThat(multiLastValueProducer.lastValue(), equalTo(multiValue));
        // Ensure we read all the available values
        assertThat(values.iterator.hasNext(), equalTo(false));
        multiLastValueProducer.reset();
        assertThat(multiLastValueProducer.lastValue(), nullValue());
    }

    public void testFlattenedLastValueFieldProducer() throws IOException {
        var producer = LastValueFieldProducer.createForLabel("dummy", "flattened");
        assertTrue(producer.isEmpty());
        assertEquals("dummy", producer.name());

        var bytes = List.of("a\0value_a", "b\0value_b", "c\0value_c", "d\0value_d");
        var docValues = new FormattedDocValues() {

            Iterator<String> iterator = bytes.iterator();

            @Override
            public boolean advanceExact(int docId) {
                return true;
            }

            @Override
            public int docValueCount() {
                return bytes.size();
            }

            @Override
            public Object nextValue() {
                return iterator.next();
            }
        };

        producer.collect(docValues, IntArrayList.from(1));
        assertFalse(producer.isEmpty());
        assertEquals("a\0value_a", (((Object[]) producer.lastValue())[0]).toString());
        assertEquals("b\0value_b", (((Object[]) producer.lastValue())[1]).toString());
        assertEquals("c\0value_c", (((Object[]) producer.lastValue())[2]).toString());
        assertEquals("d\0value_d", (((Object[]) producer.lastValue())[3]).toString());

        var builder = new XContentBuilder(XContentType.JSON.xContent(), new ByteArrayOutputStream());
        builder.startObject();
        producer.write(builder);
        builder.endObject();
        var content = Strings.toString(builder);
        assertThat(content, equalTo("{\"dummy\":{\"a\":\"value_a\",\"b\":\"value_b\",\"c\":\"value_c\",\"d\":\"value_d\"}}"));

        producer.reset();
        assertTrue(producer.isEmpty());
        assertNull(producer.lastValue());
    }

    static <T> FormattedDocValues createValuesInstance(IntArrayList docIdBuffer, T[] values) {
        return new FormattedDocValues() {

            final IntObjectHashMap<T> docIdToValue = IntObjectHashMap.from(docIdBuffer.toArray(), values);

            int currentDocId = -1;

            @Override
            public boolean advanceExact(int target) throws IOException {
                currentDocId = target;
                return docIdToValue.containsKey(target);
            }

            @Override
            public T nextValue() throws IOException {
                return docIdToValue.get(currentDocId);
            }

            @Override
            public int docValueCount() {
                return 1;
            }
        };
    }
}
