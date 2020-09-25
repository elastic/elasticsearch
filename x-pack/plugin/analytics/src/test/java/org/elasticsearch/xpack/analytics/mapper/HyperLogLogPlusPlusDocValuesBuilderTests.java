/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.mapper;

import com.carrotsearch.hppc.ByteArrayList;
import com.carrotsearch.hppc.IntArrayList;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.TestUtil;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.search.aggregations.metrics.AbstractHyperLogLog;
import org.elasticsearch.search.aggregations.metrics.AbstractHyperLogLogPlusPlus;
import org.elasticsearch.search.aggregations.metrics.AbstractLinearCounting;
import org.elasticsearch.search.aggregations.metrics.HyperLogLogPlusPlus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.analytics.mapper.fielddata.HyperLogLogPlusPlusValue;

import static org.hamcrest.CoreMatchers.equalTo;

import java.io.IOException;

public class HyperLogLogPlusPlusDocValuesBuilderTests extends ESTestCase {

    public void testRandomTiny() throws IOException {
        for (int i =0; i < 5; i++) {
            doTestRandom(randomInt(10));
        }
    }

    public void testRandomMergeTiny() throws IOException {
        doTestMerge(randomInt(10));
    }

    public void testRandomMedium() throws IOException {
        for (int i =0; i < 5; i++) {
            doTestRandom(randomIntBetween(10, 10000));
        }
    }

    public void testRandomMergeMedium() throws IOException {
        doTestMerge(randomIntBetween(10, 10000));
    }

    public void testRandomBig() throws IOException {
        for (int i =0; i < 5; i++) {
            doTestRandom(randomIntBetween(10000, 1000000));
        }
    }

    public void testRandomMergeBig() throws IOException {
        doTestMerge(randomIntBetween(10000, 1000000));
    }

    private void doTestRandom(final int values) throws IOException {
        final org.elasticsearch.common.hash.MurmurHash3.Hash128 hash = new org.elasticsearch.common.hash.MurmurHash3.Hash128();
        final int precision = randomIntBetween(AbstractHyperLogLog.MIN_PRECISION, AbstractHyperLogLog.MAX_PRECISION);
        final HyperLogLogPlusPlus counts = new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, 1);
        for (int i =0; i < values; i++) {
            counts.collect(0, randomHash(hash));
        }
        final HyperLogLogPlusPlusValue value = getHyperLogLogValue(counts);
        assertEquals(value, counts);

    }

    private void doTestMerge(int values) throws IOException {
        final org.elasticsearch.common.hash.MurmurHash3.Hash128 hash = new org.elasticsearch.common.hash.MurmurHash3.Hash128();
        final int precisionHigh = randomIntBetween(AbstractHyperLogLog.MIN_PRECISION, AbstractHyperLogLog.MAX_PRECISION);
        final int precisionLow = randomIntBetween(AbstractHyperLogLog.MIN_PRECISION, precisionHigh);
        final HyperLogLogPlusPlus countsHigh = new HyperLogLogPlusPlus(precisionHigh, BigArrays.NON_RECYCLING_INSTANCE, 1);
        final HyperLogLogPlusPlus countsLow = new HyperLogLogPlusPlus(precisionLow, BigArrays.NON_RECYCLING_INSTANCE, 1);
        final HyperLogLogPlusPlus counts = new HyperLogLogPlusPlus(precisionLow, BigArrays.NON_RECYCLING_INSTANCE, 1);

        for (int i =0; i < values; i++) {
            final long value = randomHash(hash);
            if (randomBoolean()) {
                countsHigh.collect(0, value);
            } else {
                countsLow.collect(0, value);
            }
            counts.collect(0, value);
        }
        final HyperLogLogPlusPlusValue value = getHyperLogLogValue(countsHigh);
        if (value.getAlgorithm() == HyperLogLogPlusPlusValue.Algorithm.HYPERLOGLOG) {
            countsLow.merge(0, value.getHyperLogLog());
        } else {
            countsLow.merge(0, value.getLinearCounting());
        }
        assertThat(counts.cardinality(0), equalTo(countsLow.cardinality(0)));
    }

    private void assertEquals(HyperLogLogPlusPlusValue value, HyperLogLogPlusPlus counts) {
        if (value.getAlgorithm() == HyperLogLogPlusPlusValue.Algorithm.HYPERLOGLOG) {
            assertThat(AbstractHyperLogLogPlusPlus.HYPERLOGLOG, equalTo(counts.getAlgorithm(0)));
            AbstractHyperLogLog.RunLenIterator countsIterator = counts.getHyperLogLog(0);
            AbstractHyperLogLog.RunLenIterator iterator = value.getHyperLogLog();
            for (int i = 0; i < 1 << counts.precision(); i++) {
                assertThat(true, equalTo(iterator.next()));
                assertThat(true, equalTo(countsIterator.next()));
                assertThat(countsIterator.value(), equalTo(iterator.value()));
            }
            assertThat(false, equalTo(iterator.next()));
            assertThat(false, equalTo(countsIterator.next()));
        } else {
            assertThat(AbstractHyperLogLogPlusPlus.LINEAR_COUNTING, equalTo(counts.getAlgorithm(0)));
            AbstractLinearCounting.EncodedHashesIterator countsIterator = counts.getLinearCounting(0);
            AbstractLinearCounting.EncodedHashesIterator iterator = value.getLinearCounting();
            assertThat(countsIterator.size(), equalTo(iterator.size()));
            for (int i = 0; i < countsIterator.size(); i++) {
                assertThat(true, equalTo(iterator.next()));
                assertThat(true, equalTo(countsIterator.next()));
                assertThat(countsIterator.value(), equalTo(iterator.value()));
            }
            assertThat(false, equalTo(iterator.next()));
            assertThat(false, equalTo(countsIterator.next()));
        }
    }

    private HyperLogLogPlusPlusValue getHyperLogLogValue(AbstractHyperLogLogPlusPlus counts) throws IOException {
        final ByteBuffersDataOutput dataOutput = new ByteBuffersDataOutput();
        if (counts.getAlgorithm(0) == AbstractHyperLogLogPlusPlus.HYPERLOGLOG) {
            final ByteArrayList runLens = new ByteArrayList();
            AbstractHyperLogLog.RunLenIterator iterator = counts.getHyperLogLog(0);
            while (iterator.next()) {
                runLens.add(iterator.value());
            }
            HyperLogLogPlusPlusDocValuesBuilder.writeHLL(runLens, dataOutput);
        } else {
            final IntArrayList hashes = new IntArrayList();
            AbstractLinearCounting.EncodedHashesIterator iterator = counts.getLinearCounting(0);
            while (iterator.next()) {
                hashes.add(iterator.value());
            }
            HyperLogLogPlusPlusDocValuesBuilder.writeLC(hashes, dataOutput);
        }

        final ByteArrayDataInput dataInput = new ByteArrayDataInput();
        dataInput.reset(dataOutput.toArrayCopy());
        final HyperLogLogPlusPlusDocValuesBuilder builder = new HyperLogLogPlusPlusDocValuesBuilder(counts.precision());
        return builder.decode(dataInput);
    }

    private long randomHash(org.elasticsearch.common.hash.MurmurHash3.Hash128 hash) {
        String value  = TestUtil.randomSimpleString(random());
        BytesRef ref = new BytesRef(value);
        org.elasticsearch.common.hash.MurmurHash3.hash128(ref.bytes, ref.offset, ref.length, 0, hash);
        return hash.h1;
    }
}
