/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.MockBlockFactory;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class SingletonOrdinalsBuilderTests extends ESTestCase {
    public void testReader() throws IOException {
        testRead(breakingDriverContext().blockFactory());
    }

    public void testReadWithCranky() throws IOException {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, new CrankyCircuitBreakerService());
        BlockFactory factory = new BlockFactory(bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST), bigArrays);
        try {
            testRead(factory);
            // If we made it this far cranky didn't fail us!
        } catch (CircuitBreakingException e) {
            logger.info("cranky", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
        assertThat(factory.breaker().getUsed(), equalTo(0L));
    }

    private void testRead(BlockFactory factory) throws IOException {
        int count = 1000;
        try (Directory directory = newDirectory(); RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
            for (int i = 0; i < count; i++) {
                for (BytesRef v : new BytesRef[] { new BytesRef("a"), new BytesRef("b"), new BytesRef("c"), new BytesRef("d") }) {
                    indexWriter.addDocument(List.of(new SortedDocValuesField("f", v)));
                }
            }
            Map<String, Integer> counts = new HashMap<>();
            try (IndexReader reader = indexWriter.getReader()) {
                for (LeafReaderContext ctx : reader.leaves()) {
                    SortedDocValues docValues = ctx.reader().getSortedDocValues("f");
                    try (SingletonOrdinalsBuilder builder = new SingletonOrdinalsBuilder(factory, docValues, ctx.reader().numDocs())) {
                        for (int i = 0; i < ctx.reader().maxDoc(); i++) {
                            if (ctx.reader().getLiveDocs() == null || ctx.reader().getLiveDocs().get(i)) {
                                assertThat(docValues.advanceExact(i), equalTo(true));
                                builder.appendOrd(docValues.ordValue());
                            }
                        }
                        try (BytesRefBlock build = buildOrdinalsBuilder(builder)) {
                            for (int i = 0; i < build.getPositionCount(); i++) {
                                counts.merge(build.getBytesRef(i, new BytesRef()).utf8ToString(), 1, (lhs, rhs) -> lhs + rhs);
                            }
                        }
                    }
                }
            }
            assertMap(counts, matchesMap().entry("a", count).entry("b", count).entry("c", count).entry("d", count));
        }
    }

    public void testCompactWithNulls() {
        assertCompactToUnique(new int[] { -1, -1, -1, -1, 0, 1, 2 }, List.of(0, 1, 2));
    }

    public void testCompactNoNulls() {
        assertCompactToUnique(new int[] { 0, 1, 2 }, List.of(0, 1, 2));
    }

    public void testCompactDups() {
        assertCompactToUnique(new int[] { 0, 0, 0, 1, 2 }, List.of(0, 1, 2));
    }

    public void testCompactSkips() {
        assertCompactToUnique(new int[] { 2, 7, 1000 }, List.of(2, 7, 1000));
    }

    private void assertCompactToUnique(int[] sortedOrds, List<Integer> expected) {
        int uniqueLength = SingletonOrdinalsBuilder.compactToUnique(sortedOrds);
        assertMap(Arrays.stream(sortedOrds).mapToObj(Integer::valueOf).limit(uniqueLength).toList(), matchesList(expected));
    }

    private final List<CircuitBreaker> breakers = new ArrayList<>();
    private final List<BlockFactory> blockFactories = new ArrayList<>();

    /**
     * A {@link DriverContext} with a breaking {@link BigArrays} and {@link BlockFactory}.
     */
    protected DriverContext breakingDriverContext() { // TODO move this to driverContext once everyone supports breaking
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofGb(1)).withCircuitBreaking();
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        breakers.add(breaker);
        BlockFactory factory = new MockBlockFactory(breaker, bigArrays);
        blockFactories.add(factory);
        return new DriverContext(bigArrays, factory);
    }

    public void testAllNull() throws IOException {
        BlockFactory factory = breakingDriverContext().blockFactory();
        int count = 1000;
        try (Directory directory = newDirectory(); RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
            for (int i = 0; i < count; i++) {
                for (BytesRef v : new BytesRef[] { new BytesRef("a"), new BytesRef("b"), new BytesRef("c"), new BytesRef("d") }) {
                    indexWriter.addDocument(List.of(new SortedDocValuesField("f", v)));
                }
            }
            try (IndexReader reader = indexWriter.getReader()) {
                for (LeafReaderContext ctx : reader.leaves()) {
                    SortedDocValues docValues = ctx.reader().getSortedDocValues("f");
                    try (SingletonOrdinalsBuilder builder = new SingletonOrdinalsBuilder(factory, docValues, ctx.reader().numDocs())) {
                        for (int i = 0; i < ctx.reader().maxDoc(); i++) {
                            if (ctx.reader().getLiveDocs() == null || ctx.reader().getLiveDocs().get(i)) {
                                assertThat(docValues.advanceExact(i), equalTo(true));
                                builder.appendNull();
                            }
                        }
                        try (BytesRefBlock built = buildOrdinalsBuilder(builder)) {
                            for (int p = 0; p < built.getPositionCount(); p++) {
                                assertThat(built.isNull(p), equalTo(true));
                            }
                            assertThat(built.areAllValuesNull(), equalTo(true));
                        }
                    }
                }
            }
        }
    }

    public void testEmitOrdinalForHighCardinality() throws IOException {
        BlockFactory factory = breakingDriverContext().blockFactory();
        int numOrds = between(50, 100);
        try (Directory directory = newDirectory(); IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
            for (int o = 0; o < numOrds; o++) {
                int docPerOrds = between(10, 15);
                for (int d = 0; d < docPerOrds; d++) {
                    indexWriter.addDocument(List.of(new SortedDocValuesField("f", new BytesRef("value-" + o))));
                }
            }
            try (IndexReader reader = DirectoryReader.open(indexWriter)) {
                assertThat(reader.leaves(), hasSize(1));
                for (LeafReaderContext ctx : reader.leaves()) {
                    int batchSize = between(40, 100);
                    int ord = random().nextInt(numOrds);
                    try (
                        var b1 = new SingletonOrdinalsBuilder(factory, ctx.reader().getSortedDocValues("f"), batchSize);
                        var b2 = new SingletonOrdinalsBuilder(factory, ctx.reader().getSortedDocValues("f"), batchSize)
                    ) {
                        for (int i = 0; i < batchSize; i++) {
                            b1.appendOrd(ord);
                            b2.appendOrd(ord);
                        }
                        try (BytesRefBlock block1 = b1.build(); BytesRefBlock block2 = b2.buildRegularBlock()) {
                            assertThat(block1, equalTo(block2));
                            assertNotNull(block1.asOrdinals());
                        }
                    }
                }
            }
        }
    }

    static BytesRefBlock buildOrdinalsBuilder(SingletonOrdinalsBuilder builder) {
        if (randomBoolean()) {
            return builder.buildRegularBlock();
        } else {
            return builder.buildOrdinal();
        }
    }

    @After
    public void allBreakersEmpty() throws Exception {
        // first check that all big arrays are released, which can affect breakers
        MockBigArrays.ensureAllArraysAreReleased();

        for (CircuitBreaker breaker : breakers) {
            for (var factory : blockFactories) {
                if (factory instanceof MockBlockFactory mockBlockFactory) {
                    mockBlockFactory.ensureAllBlocksAreReleased();
                }
            }
            assertThat("Unexpected used in breaker: " + breaker, breaker.getUsed(), equalTo(0L));
        }
    }

}
