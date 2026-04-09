/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.apache.lucene.internal.hppc.IntArrayList;
import org.apache.lucene.internal.hppc.IntObjectHashMap;
import org.elasticsearch.index.fielddata.FormattedDocValues;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class DimensionFieldProducerTests extends ESTestCase {

    public void testKeywordDimension() throws IOException {
        String name = randomAlphanumericOfLength(10);
        DimensionFieldProducer dimensionProducer = new DimensionFieldProducer(name, new DimensionFieldProducer.Dimension(name));
        assertThat(dimensionProducer.dimensionValues(), nullValue());
        var docIdBuffer = IntArrayList.from(0, 1, 2);
        var values = createValuesInstance(docIdBuffer, new String[] { "aaa", "aaa", "aaa" });
        dimensionProducer.collect(values, docIdBuffer);
        assertThat(dimensionProducer.dimensionValues(), equalTo("aaa"));
        dimensionProducer.reset();
        assertThat(dimensionProducer.dimensionValues(), nullValue());
    }

    public void testDoubleDimension() throws IOException {
        String name = randomAlphanumericOfLength(10);
        DimensionFieldProducer dimensionProducer = new DimensionFieldProducer(name, new DimensionFieldProducer.Dimension(name));
        assertThat(dimensionProducer.dimensionValues(), nullValue());
        var docIdBuffer = IntArrayList.from(0, 1, 2);
        var values = createValuesInstance(docIdBuffer, new Double[] { 10.20D, 10.20D, 10.20D });
        dimensionProducer.collect(values, docIdBuffer);
        assertThat(dimensionProducer.dimensionValues(), equalTo(10.20D));
        dimensionProducer.reset();
        assertThat(dimensionProducer.dimensionValues(), nullValue());
    }

    public void testIntegerDimension() throws IOException {
        String name = randomAlphanumericOfLength(10);
        DimensionFieldProducer dimensionProducer = new DimensionFieldProducer(name, new DimensionFieldProducer.Dimension(name));
        assertThat(dimensionProducer.dimensionValues(), nullValue());
        var docIdBuffer = IntArrayList.from(0, 1, 2);
        var values = createValuesInstance(docIdBuffer, new Integer[] { 10, 10, 10 });
        dimensionProducer.collect(values, docIdBuffer);
        assertThat(dimensionProducer.dimensionValues(), equalTo(10));
        dimensionProducer.reset();
        assertThat(dimensionProducer.dimensionValues(), nullValue());
    }

    public void testBooleanDimension() throws IOException {
        String name = randomAlphanumericOfLength(10);
        DimensionFieldProducer dimensionProducer = new DimensionFieldProducer(name, new DimensionFieldProducer.Dimension(name));
        assertThat(dimensionProducer.dimensionValues(), nullValue());
        var docIdBuffer = IntArrayList.from(0, 1, 2);
        var values = createValuesInstance(docIdBuffer, new Boolean[] { true, true, true });
        dimensionProducer.collect(values, docIdBuffer);
        assertThat(dimensionProducer.dimensionValues(), equalTo(true));
        dimensionProducer.reset();
        assertThat(dimensionProducer.dimensionValues(), nullValue());
    }

    public void testMultiValueDimensions() throws IOException {
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

        values.iterator = Arrays.stream(multiValue).iterator();
        String name = randomAlphanumericOfLength(10);
        DimensionFieldProducer multiLastValueProducer = new DimensionFieldProducer(name, new DimensionFieldProducer.Dimension(name));
        assertThat(multiLastValueProducer.dimensionValues(), nullValue());
        multiLastValueProducer.collect(values, docIdBuffer);
        assertThat(multiLastValueProducer.dimensionValues(), instanceOf(Object[].class));
        assertThat((Object[]) multiLastValueProducer.dimensionValues(), arrayContainingInAnyOrder(true, false));
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
