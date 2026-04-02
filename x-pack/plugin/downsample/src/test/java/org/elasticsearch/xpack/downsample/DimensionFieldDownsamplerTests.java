/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.apache.lucene.internal.hppc.IntArrayList;
import org.elasticsearch.index.fielddata.FormattedDocValues;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import static org.elasticsearch.xpack.downsample.LastValueFieldDownsamplerTests.createValuesInstance;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class DimensionFieldDownsamplerTests extends ESTestCase {

    public void testKeywordDimension() throws IOException {
        DimensionFieldDownsampler dimensionDownsampler = new DimensionFieldDownsampler(randomAlphanumericOfLength(10), null, null);
        assertThat(dimensionDownsampler.lastValue(), nullValue());
        var docIdBuffer = IntArrayList.from(0, 1, 2);
        var values = createValuesInstance(docIdBuffer, new String[] { "aaa", "aaa", "aaa" });
        dimensionDownsampler.collect(values, docIdBuffer);
        assertThat(dimensionDownsampler.lastValue(), equalTo("aaa"));
        dimensionDownsampler.reset();
        assertThat(dimensionDownsampler.lastValue(), nullValue());
    }

    public void testDoubleDimension() throws IOException {
        DimensionFieldDownsampler dimensionDownsampler = new DimensionFieldDownsampler(randomAlphanumericOfLength(10), null, null);
        assertThat(dimensionDownsampler.lastValue(), nullValue());
        var docIdBuffer = IntArrayList.from(0, 1, 2);
        var values = createValuesInstance(docIdBuffer, new Double[] { 10.20D, 10.20D, 10.20D });
        dimensionDownsampler.collect(values, docIdBuffer);
        assertThat(dimensionDownsampler.lastValue(), equalTo(10.20D));
        dimensionDownsampler.reset();
        assertThat(dimensionDownsampler.lastValue(), nullValue());
    }

    public void testIntegerDimension() throws IOException {
        DimensionFieldDownsampler dimensionDownsampler = new DimensionFieldDownsampler(randomAlphanumericOfLength(10), null, null);
        assertThat(dimensionDownsampler.lastValue(), nullValue());
        var docIdBuffer = IntArrayList.from(0, 1, 2);
        var values = createValuesInstance(docIdBuffer, new Integer[] { 10, 10, 10 });
        dimensionDownsampler.collect(values, docIdBuffer);
        assertThat(dimensionDownsampler.lastValue(), equalTo(10));
        dimensionDownsampler.reset();
        assertThat(dimensionDownsampler.lastValue(), nullValue());
    }

    public void testBooleanDimension() throws IOException {
        DimensionFieldDownsampler dimensionDownsampler = new DimensionFieldDownsampler(randomAlphanumericOfLength(10), null, null);
        assertThat(dimensionDownsampler.lastValue(), nullValue());
        var docIdBuffer = IntArrayList.from(0, 1, 2);
        var values = createValuesInstance(docIdBuffer, new Boolean[] { true, true, true });
        dimensionDownsampler.collect(values, docIdBuffer);
        assertThat(dimensionDownsampler.lastValue(), equalTo(true));
        dimensionDownsampler.reset();
        assertThat(dimensionDownsampler.lastValue(), nullValue());
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
        DimensionFieldDownsampler multiLastValueProducer = new DimensionFieldDownsampler(randomAlphanumericOfLength(10), null, null);
        assertThat(multiLastValueProducer.lastValue(), nullValue());
        multiLastValueProducer.collect(values, docIdBuffer);
        assertThat(multiLastValueProducer.lastValue(), instanceOf(Object[].class));
        assertThat((Object[]) multiLastValueProducer.lastValue(), arrayContainingInAnyOrder(true, false));
    }
}
