/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.mapper;

import com.carrotsearch.hppc.ByteArrayList;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.elasticsearch.search.aggregations.metrics.AbstractHyperLogLog;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.analytics.mapper.fielddata.HllValue;

import static org.hamcrest.CoreMatchers.equalTo;

import java.io.IOException;

public class HLLDocValuesBuilderTests extends ESTestCase {

    public void testRandomTiny() throws IOException {
        for (int i =0; i < 5; i++) {
            doTestRandom(randomInt(10));
        }
    }

    public void testRandomMedium() throws IOException {
        for (int i =0; i < 5; i++) {
            doTestRandom(randomIntBetween(10, 10000));
        }
    }

    public void testRandomBig() throws IOException {
        for (int i =0; i < 5; i++) {
            doTestRandom(randomIntBetween(10000, 1000000));
        }
    }

    private void doTestRandom(final int values) throws IOException {
        final int precision = randomIntBetween(AbstractHyperLogLog.MIN_PRECISION, AbstractHyperLogLog.MAX_PRECISION);
        final int numberRegisters = 1 << precision;
        final SimpleHyperLogLog counts = new SimpleHyperLogLog(precision);

        for (int i =0; i < values; i++) {
            counts.collect(0, randomLong());
        }
        final ByteBuffersDataOutput dataOutput = new ByteBuffersDataOutput();
        final ByteArrayList runLens = new ByteArrayList();
        runLens.add(counts.runLens);
        HLLDocValuesBuilder.writeCompressed(runLens, dataOutput);
        final ByteArrayDataInput dataInput = new ByteArrayDataInput();
        dataInput.reset(dataOutput.toArrayCopy());
        final HLLDocValuesBuilder builder = new HLLDocValuesBuilder();
        final HllValue value = builder.decode(dataInput);
        for (int i = 0; i < numberRegisters; i++) {
            assertThat(true, equalTo(value.next()));
            assertThat(runLens.get(i), equalTo(value.value()));
        }
        assertThat(false, equalTo(value.next()));
    }

    private static class SimpleHyperLogLog extends AbstractHyperLogLog {

        protected final byte[] runLens;

        SimpleHyperLogLog(int precision) {
            super(precision);
            runLens = new byte[m];
        }

        @Override
        protected void addRunLen(long bucketOrd, int register, int runLen) {
            runLens[register] =  (byte) Math.max(runLen, runLens[register]);
        }

        @Override
        protected RunLenIterator getRunLens(long bucketOrd) {
            throw new UnsupportedOperationException();
        }
    }
}
