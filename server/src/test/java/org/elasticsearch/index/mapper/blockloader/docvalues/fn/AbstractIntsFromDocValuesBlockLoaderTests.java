/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues.fn;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.TestBlock;
import org.elasticsearch.index.mapper.blockloader.CrankyDirectoryReader;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public abstract class AbstractIntsFromDocValuesBlockLoaderTests extends ESTestCase {
    @ParametersFactory(argumentFormatting = "blockAtATime=%s, multiValues=%s, missingValues=%s")
    public static List<Object[]> parameters() throws IOException {
        List<Object[]> parameters = new ArrayList<>();
        for (boolean blockAtATime : new boolean[] { true, false }) {
            for (boolean multiValues : new boolean[] { true, false }) {
                for (boolean missingValues : new boolean[] { true, false }) {
                    parameters.add(new Object[] { blockAtATime, multiValues, missingValues });
                }
            }
        }
        return parameters;
    }

    protected final boolean blockAtATime;
    protected final boolean multiValues;
    protected final boolean missingValues;

    public AbstractIntsFromDocValuesBlockLoaderTests(boolean blockAtATime, boolean multiValues, boolean missingValues) {
        this.blockAtATime = blockAtATime;
        this.multiValues = multiValues;
        this.missingValues = missingValues;
    }

    protected abstract void innerTest(CircuitBreaker breaker, LeafReaderContext ctx, int mvCount) throws IOException;

    public void test() throws IOException {
        test(newLimitedBreaker(ByteSizeValue.ofMb(1)), r -> r);
    }

    public void testWithCranky() throws IOException {
        CircuitBreaker cranky = new CrankyCircuitBreakerService.CrankyCircuitBreaker();
        try {
            test(cranky, r -> r);
            logger.info("Cranky breaker didn't break. This should be rare, but possible randomly.");
        } catch (CircuitBreakingException e) {
            logger.info("Cranky breaker broke", e);
        }
        assertThat(cranky.getUsed(), equalTo(0L));
    }

    public void testWithCrankyReader() {
        try {
            test(newLimitedBreaker(ByteSizeValue.ofMb(10)), CrankyDirectoryReader::new);
            logger.info("Cranky reader didn't break. This should be rare, but possible randomly.");
        } catch (IOException e) {
            logger.info("Cranky reader broke", e);
        }
    }

    public void testWithCrankyBreakerAndReader() {
        CircuitBreaker cranky = new CrankyCircuitBreakerService.CrankyCircuitBreaker();
        try {
            test(cranky, CrankyDirectoryReader::new);
            logger.info("Cranky breaker nor reader didn't break. This should be rare, but possible randomly.");
        } catch (IOException | CircuitBreakingException e) {
            logger.info("Cranky breaker or reader broke", e);
        }
        assertThat(cranky.getUsed(), equalTo(0L));
    }

    private void test(CircuitBreaker breaker, CheckedFunction<DirectoryReader, DirectoryReader, IOException> wrap) throws IOException {
        int mvCount = 0;
        try (Directory dir = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {
            int docCount = 10_000;
            for (int i = 0; i < docCount; i++) {
                List<IndexableField> doc = new ArrayList<>(2);
                doc.add(field(i));
                if (multiValues && i % 100 == 0) {
                    doc.add(field((i % 100) + 1));
                    mvCount++;
                }
                iw.addDocument(doc);
            }
            if (missingValues) {
                iw.addDocument(List.of());
            }
            iw.forceMerge(1);
            try (DirectoryReader dr = wrap.apply(iw.getReader())) {
                LeafReaderContext ctx = getOnlyLeafReader(dr).getContext();
                innerTest(breaker, ctx, mvCount);
            }
        }
    }

    protected final TestBlock read(BlockLoader loader, BlockLoader.AllReader reader, LeafReaderContext ctx, BlockLoader.Docs docs)
        throws IOException {
        BlockLoader.AllReader toUse = blockAtATime
            ? reader
            : new ForceDocAtATime(() -> loader.builder(TestBlock.factory(), docs.count()), reader);

        return (TestBlock) toUse.read(TestBlock.factory(), docs, 0, false);
    }

    private static IntField field(int v) {
        return new IntField("field", v, Field.Store.NO);
    }
}
