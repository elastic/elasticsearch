/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.index.DirectoryReader;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

/**
 * Testing helper for implementation of {@link BlockLoader}.
 */
public abstract class AbstractBlockLoaderTestCase extends ESTestCase {
    @ParametersFactory(argumentFormatting = "multiValues=%s, missingValues=%s")
    public static List<Object[]> parameters() throws IOException {
        List<Object[]> parameters = new ArrayList<>();
        for (boolean multiValues : new boolean[] { true, false }) {
            for (boolean missingValues : new boolean[] { true, false }) {
                parameters.add(new Object[] { multiValues, missingValues });
            }
        }
        return parameters;
    }

    protected final boolean multiValues;
    protected final boolean missingValues;

    public AbstractBlockLoaderTestCase(boolean multiValues, boolean missingValues) {
        this.multiValues = multiValues;
        this.missingValues = missingValues;
    }

    public final void test() throws IOException {
        test(newLimitedBreaker(ByteSizeValue.ofMb(5)), r -> r);
    }

    public final void testWithCrankyBreaker() throws IOException {
        CircuitBreaker cranky = new CrankyCircuitBreakerService.CrankyCircuitBreaker();
        try {
            test(cranky, r -> r);
            logger.info("Cranky breaker didn't break. This should be rare, but possible randomly.");
        } catch (CircuitBreakingException e) {
            logger.info("Cranky breaker broke", e);
        }
        assertThat(cranky.getUsed(), equalTo(0L));
    }

    public final void testWithCrankyReader() {
        try {
            test(newLimitedBreaker(ByteSizeValue.ofMb(10)), CrankyDirectoryReader::new);
            logger.info("Cranky reader didn't break.");
        } catch (IOException e) {
            logger.info("Cranky reader broke", e);
        }
    }

    public final void testWithCrankyBreakerAndReader() {
        CircuitBreaker cranky = new CrankyCircuitBreakerService.CrankyCircuitBreaker();
        try {
            test(cranky, CrankyDirectoryReader::new);
            logger.info("Cranky breaker nor reader didn't break. This should be rare, but possible randomly.");
        } catch (IOException | CircuitBreakingException e) {
            logger.info("Cranky breaker or reader broke", e);
        }
        assertThat(cranky.getUsed(), equalTo(0L));
    }

    protected abstract void test(CircuitBreaker breaker, CheckedFunction<DirectoryReader, DirectoryReader, IOException> wrap)
        throws IOException;

    /**
     * Read from the reader.
     * <p>
     *     About half the time this just calls {@link BlockLoader.ColumnAtATimeReader#read}
     *     to read the entire block at once. That's the "fast path". The path that's most
     *     common for loading before an agg. The other half the time it reads in smaller
     *     chunks to similar loading from jumbled segments. That's the path that's common
     *     to load after a TopN.
     * </p>
     */
    protected final TestBlock read(BlockLoader.ColumnAtATimeReader reader, BlockLoader.Docs docs) throws IOException {
        if (randomBoolean()) {
            return (TestBlock) reader.read(TestBlock.factory(), docs, 0, false);
        }
        List<Object> results = new ArrayList<>(docs.count());
        int step = randomBoolean() ? 1 : between(2, 100);
        for (int offset = 0; offset < docs.count(); offset += step) {
            int finalOffset = offset;
            BlockLoader.Docs subDocs = new BlockLoader.Docs() {
                @Override
                public int count() {
                    return Math.min(step, docs.count() - finalOffset) + finalOffset;
                }

                @Override
                public int get(int i) {
                    return docs.get(i);
                }

                @Override
                public boolean mayContainDuplicates() {
                    return docs.mayContainDuplicates();
                }
            };
            TestBlock subResults = (TestBlock) reader.read(TestBlock.factory(), subDocs, offset, false);
            for (int r = 0; r < subResults.size(); r++) {
                results.add(subResults.get(r));
            }
        }
        return new TestBlock(results);
    }
}
