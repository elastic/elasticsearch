/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.mapper;

import com.carrotsearch.hppc.ByteArrayList;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.TestUtil;
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
        final org.elasticsearch.common.hash.MurmurHash3.Hash128 hash = new org.elasticsearch.common.hash.MurmurHash3.Hash128();
        final int precision = randomIntBetween(AbstractHyperLogLog.MIN_PRECISION, AbstractHyperLogLog.MAX_PRECISION);
        final int numberRegisters = 1 << precision;
        final SimpleHyperLogLog counts = new SimpleHyperLogLog(precision);

        for (int i =0; i < values; i++) {
            counts.collect(0, randomHash(hash));
        }
        final HllValue value = getHLLValue(counts);
        for (int i = 0; i < numberRegisters; i++) {
            assertThat(true, equalTo(value.next()));
            assertThat(counts.runLens[i], equalTo(value.value()));
        }
        assertThat(false, equalTo(value.next()));
    }

    public void testMergeRegisters() throws IOException {
        final org.elasticsearch.common.hash.MurmurHash3.Hash128 hash = new org.elasticsearch.common.hash.MurmurHash3.Hash128();
        final int precisionHigh = randomIntBetween(AbstractHyperLogLog.MIN_PRECISION, AbstractHyperLogLog.MAX_PRECISION);
        final int precisionLow = randomIntBetween(AbstractHyperLogLog.MIN_PRECISION, precisionHigh);
        final int precisionDiff = precisionHigh - precisionLow;
        final int registersToMerge = 1 << precisionDiff;
        final SimpleHyperLogLog countsHigh = new SimpleHyperLogLog(precisionHigh);
        final SimpleHyperLogLog countsLow = new SimpleHyperLogLog(precisionLow);
        final SimpleHyperLogLog counts = new SimpleHyperLogLog(precisionLow);

        final int values = randomIntBetween(10000, 1000000);
        for (int i =0; i < values; i++) {
            final long value = randomHash(hash);
            if (randomBoolean()) {
                countsHigh.collect(0, value);
            } else {
                countsLow.collect(0, value);
            }
            counts.collect(0, value);
        }
        final HllValue value = getHLLValue(countsHigh);
        for (int i = 0; i < 1 << precisionLow; i++) {
            final byte runLen = value.mergeRegisters(precisionDiff, registersToMerge);
            countsLow.addRunLen(0, i, runLen);
        }
        assertThat(counts.runLens, equalTo(countsLow.runLens));
    }

    private HllValue getHLLValue(SimpleHyperLogLog counts) throws IOException {
        final ByteBuffersDataOutput dataOutput = new ByteBuffersDataOutput();
        final ByteArrayList runLens = new ByteArrayList();
        runLens.add(counts.runLens);
        HLLDocValuesBuilder.writeCompressed(runLens, dataOutput);
        final ByteArrayDataInput dataInput = new ByteArrayDataInput();
        dataInput.reset(dataOutput.toArrayCopy());
        final HLLDocValuesBuilder builder = new HLLDocValuesBuilder();
        return builder.decode(dataInput);
    }

    private long randomHash(org.elasticsearch.common.hash.MurmurHash3.Hash128 hash) {
        String value  = TestUtil.randomSimpleString(random());
        BytesRef ref = new BytesRef(value);
        org.elasticsearch.common.hash.MurmurHash3.hash128(ref.bytes, ref.offset, ref.length, 0, hash);
        return hash.h1;
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
