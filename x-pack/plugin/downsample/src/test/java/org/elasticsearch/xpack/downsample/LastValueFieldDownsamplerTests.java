/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.internal.hppc.IntArrayList;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.fielddata.FormattedDocValues;
import org.elasticsearch.index.mapper.IndexType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.downsample.FormattedDocValuesTestUtils.DocValuesType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.downsample.FormattedDocValuesTestUtils.trackingWithDocIdIterator;
import static org.elasticsearch.xpack.downsample.FormattedDocValuesTestUtils.withDocIdIterator;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class LastValueFieldDownsamplerTests extends AggregatorTestCase {

    private final DocValuesType docValuesType;

    public LastValueFieldDownsamplerTests(DocValuesType docValuesType) {
        this.docValuesType = docValuesType;
    }

    @ParametersFactory(shuffle = false)
    public static List<Object[]> iteratorTypes() {
        return List.of(new Object[] { DocValuesType.WITH_ITERATOR }, new Object[] { DocValuesType.WITHOUT_ITERATOR });
    }

    private FormattedDocValues getValues(IntArrayList docIdBuffer, Object[] values) {
        return switch (docValuesType) {
            case WITH_ITERATOR -> withDocIdIterator(docIdBuffer, values);
            case WITHOUT_ITERATOR -> FormattedDocValuesTestUtils.createValuesInstance(docIdBuffer, values);
        };
    }

    public void testLastValueKeyword() throws IOException {
        LastValueFieldDownsampler lastValueFieldProducer = new LastValueFieldDownsampler(randomAlphanumericOfLength(10), null, null);
        assertThat(lastValueFieldProducer.lastValue(), nullValue());
        var docIdBuffer = IntArrayList.from(0, 1, 2);
        var values = getValues(docIdBuffer, new String[] { "aaa", "bbb", "ccc" });
        lastValueFieldProducer.collect(values, docIdBuffer);
        assertThat(lastValueFieldProducer.lastValue(), equalTo("aaa"));
        assertThat(lastValueFieldProducer.isDone(), equalTo(true));
        lastValueFieldProducer.reset();
        assertThat(lastValueFieldProducer.isDone(), equalTo(false));
        assertThat(lastValueFieldProducer.lastValue(), nullValue());
    }

    public void testLastValueDouble() throws IOException {
        LastValueFieldDownsampler lastValueFieldProducer = new LastValueFieldDownsampler(randomAlphanumericOfLength(10), null, null);
        assertThat(lastValueFieldProducer.lastValue(), nullValue());
        var docIdBuffer = IntArrayList.from(0, 1, 2);
        var values = getValues(docIdBuffer, new Double[] { 10.20D, 17.30D, 12.60D });
        lastValueFieldProducer.collect(values, docIdBuffer);
        assertThat(lastValueFieldProducer.lastValue(), equalTo(10.20D));
        assertThat(lastValueFieldProducer.isDone(), equalTo(true));
        lastValueFieldProducer.reset();
        assertThat(lastValueFieldProducer.isDone(), equalTo(false));
        assertThat(lastValueFieldProducer.lastValue(), nullValue());
    }

    public void testLastValueInteger() throws IOException {
        LastValueFieldDownsampler lastValueFieldProducer = new LastValueFieldDownsampler(randomAlphanumericOfLength(10), null, null);
        assertThat(lastValueFieldProducer.lastValue(), nullValue());
        var docIdBuffer = IntArrayList.from(0, 1, 2);
        var values = getValues(docIdBuffer, new Integer[] { 10, 17, 12 });
        lastValueFieldProducer.collect(values, docIdBuffer);
        assertThat(lastValueFieldProducer.lastValue(), equalTo(10));
        assertThat(lastValueFieldProducer.isDone(), equalTo(true));
        lastValueFieldProducer.reset();
        assertThat(lastValueFieldProducer.isDone(), equalTo(false));
        assertThat(lastValueFieldProducer.lastValue(), nullValue());
    }

    public void testLastValueLong() throws IOException {
        LastValueFieldDownsampler lastValueFieldProducer = new LastValueFieldDownsampler(randomAlphanumericOfLength(10), null, null);
        assertThat(lastValueFieldProducer.lastValue(), nullValue());
        var docIdBuffer = IntArrayList.from(0, 1, 2);
        var values = getValues(docIdBuffer, new Long[] { 10L, 17L, 12L });
        lastValueFieldProducer.collect(values, docIdBuffer);
        assertThat(lastValueFieldProducer.lastValue(), equalTo(10L));
        assertThat(lastValueFieldProducer.isDone(), equalTo(true));
        lastValueFieldProducer.reset();
        assertThat(lastValueFieldProducer.isDone(), equalTo(false));
        assertThat(lastValueFieldProducer.lastValue(), nullValue());
    }

    public void testLastValueBoolean() throws IOException {
        LastValueFieldDownsampler lastValueFieldProducer = new LastValueFieldDownsampler(randomAlphanumericOfLength(10), null, null);
        assertThat(lastValueFieldProducer.lastValue(), nullValue());
        var docIdBuffer = IntArrayList.from(0, 1, 2);
        var values = getValues(docIdBuffer, new Boolean[] { true, false, false });
        lastValueFieldProducer.collect(values, docIdBuffer);
        assertThat(lastValueFieldProducer.lastValue(), equalTo(true));
        assertThat(lastValueFieldProducer.isDone(), equalTo(true));
        lastValueFieldProducer.reset();
        assertThat(lastValueFieldProducer.isDone(), equalTo(false));
        assertThat(lastValueFieldProducer.lastValue(), nullValue());
    }

    public void testLastValueMultiValue() throws IOException {
        var docIdBuffer = IntArrayList.from(0);
        Boolean[] multiValue = new Boolean[] { true, false };
        var innerValues = new FormattedDocValues() {

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

        var values = docValuesType == DocValuesType.WITH_ITERATOR ? withDocIdIterator(docIdBuffer, innerValues) : innerValues;

        innerValues.iterator = Arrays.stream(multiValue).iterator();
        LastValueFieldDownsampler multiLastValueProducer = new LastValueFieldDownsampler(randomAlphanumericOfLength(10), null, null);
        assertThat(multiLastValueProducer.lastValue(), nullValue());
        multiLastValueProducer.collect(values, docIdBuffer);
        assertThat(multiLastValueProducer.lastValue(), equalTo(multiValue));
        // Ensure we read all the available values
        assertThat(innerValues.iterator.hasNext(), equalTo(false));
        multiLastValueProducer.reset();
        assertThat(multiLastValueProducer.lastValue(), nullValue());
    }

    public void testFlattenedLastValueFieldDownsampler() throws IOException {
        AbstractFieldDownsampler.DownsamplerCountPerValueType fieldCounts = new AbstractFieldDownsampler.DownsamplerCountPerValueType();
        var downsampler = LastValueFieldDownsampler.create("dummy", createDummyFlattenedFieldType(), null, fieldCounts);
        assertTrue(downsampler.isEmpty());
        assertEquals("dummy", downsampler.name());
        assertEquals(1, fieldCounts.formattedValueFields());

        var bytes = List.of("a\0value_a", "b\0value_b", "c\0value_c", "d\0value_d");
        var innerDocValues = new FormattedDocValues() {

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

        var docValues = docValuesType == DocValuesType.WITH_ITERATOR
            ? withDocIdIterator(IntArrayList.from(1), innerDocValues)
            : innerDocValues;

        downsampler.collect(docValues, IntArrayList.from(1));
        assertFalse(downsampler.isEmpty());
        assertEquals("a\0value_a", (((Object[]) downsampler.lastValue())[0]).toString());
        assertEquals("b\0value_b", (((Object[]) downsampler.lastValue())[1]).toString());
        assertEquals("c\0value_c", (((Object[]) downsampler.lastValue())[2]).toString());
        assertEquals("d\0value_d", (((Object[]) downsampler.lastValue())[3]).toString());

        var builder = new XContentBuilder(XContentType.JSON.xContent(), new ByteArrayOutputStream());
        builder.startObject();
        downsampler.write(builder);
        builder.endObject();
        var content = Strings.toString(builder);
        assertThat(content, equalTo("{\"dummy\":{\"a\":\"value_a\",\"b\":\"value_b\",\"c\":\"value_c\",\"d\":\"value_d\"}}"));

        downsampler.reset();
        assertTrue(downsampler.isEmpty());
        assertNull(downsampler.lastValue());
    }

    public void testLastValueWithDocIdIterator() throws IOException {
        assumeTrue("This test is relevant only for doc values with iterator", docValuesType == DocValuesType.WITH_ITERATOR);
        var producer = new LastValueFieldDownsampler(randomAlphanumericOfLength(10), null, null);
        var docIdBuffer = IntArrayList.from(0, 1, 2, 3, 4, 5);
        var valuesInstance = trackingWithDocIdIterator(IntArrayList.from(1, 4), new String[] { "value_1", "value_4" });
        producer.collect(valuesInstance, docIdBuffer);

        assertThat(producer.isDone(), equalTo(true));
        assertThat(producer.lastValue(), equalTo("value_1"));
        assertThat(valuesInstance.advanceCalls(), equalTo(1));
        assertThat(valuesInstance.advanceExactCalls(), equalTo(0));

        // A reset should not influence an exhausted leaf
        boolean isReset = randomBoolean();
        if (isReset) {
            producer.reset();
        }
        assertThat(producer.isEmpty(), equalTo(isReset));

        producer.collect(valuesInstance, IntArrayList.from(6, 7));
        assertThat(valuesInstance.advanceCalls(), equalTo(1));
        assertThat(valuesInstance.advanceExactCalls(), equalTo(0));
        assertThat(producer.isEmpty(), equalTo(isReset));

        producer.reset();
        // A second leaf should be processed separetely
        var secondLeafValues = trackingWithDocIdIterator(IntArrayList.from(4), new String[] { "value_4" });
        producer.collect(secondLeafValues, IntArrayList.from(4, 5));

        assertFalse(producer.isEmpty());
        assertThat(producer.lastValue(), equalTo("value_4"));
        assertEquals(1, secondLeafValues.advanceCalls());
        assertEquals(0, secondLeafValues.advanceExactCalls());
    }

    private static MappedFieldType createDummyFlattenedFieldType() {
        return new MappedFieldType("dummy", IndexType.NONE, false, Map.of()) {
            @Override
            public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
                return null;
            }

            @Override
            public String typeName() {
                return "flattened";
            }

            @Override
            public Query termQuery(Object value, SearchExecutionContext context) {
                return null;
            }
        };
    }
}
