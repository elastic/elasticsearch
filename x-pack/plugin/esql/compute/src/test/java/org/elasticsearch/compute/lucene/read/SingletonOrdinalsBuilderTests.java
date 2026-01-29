/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.read;

import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.indices.CrankyCircuitBreakerService;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class SingletonOrdinalsBuilderTests extends ComputeTestCase {

    public void testReader() throws IOException {
        testRead(blockFactory(), between(10, 1000), randomBoolean());
    }

    public void testReadWithCranky() throws IOException {
        var factory = crankyBlockFactory();
        try {
            testRead(factory, between(10, 1000), randomBoolean());
            // If we made it this far cranky didn't fail us!
        } catch (CircuitBreakingException e) {
            logger.info("cranky", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
        assertThat(factory.breaker().getUsed(), equalTo(0L));
    }

    private void testRead(BlockFactory factory, int docCount, boolean withIndexSorts) throws IOException {
        var config = new IndexWriterConfig();
        if (withIndexSorts) {
            config.setIndexSort(new Sort(new SortField("f", SortField.Type.STRING)));
        }
        try (Directory directory = newDirectory(); RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory, config)) {
            for (int i = 0; i < docCount; i++) {
                for (BytesRef v : new BytesRef[] { new BytesRef("a"), new BytesRef("b"), new BytesRef("c"), new BytesRef("d") }) {
                    indexWriter.addDocument(List.of(new SortedDocValuesField("f", v)));
                }
            }
            Map<String, Integer> counts = new HashMap<>();
            var keywordField = new KeywordFieldMapper.KeywordFieldType("f");
            var blockLoader = keywordField.blockLoader(ValuesSourceReaderOperatorTests.blContext());
            var blockFactory = new ComputeBlockLoaderFactory(factory);
            try (IndexReader reader = indexWriter.getReader()) {
                for (LeafReaderContext ctx : reader.leaves()) {
                    int start = 0;
                    int numDocs = ctx.reader().numDocs();
                    while (start < numDocs) {
                        int end = start + randomIntBetween(1, numDocs - start);
                        BlockLoader.Docs docs = new BlockLoader.Docs() {
                            @Override
                            public int count() {
                                return end;
                            }

                            @Override
                            public int get(int i) {
                                return i;
                            }
                        };
                        var columnAtATimeReader = blockLoader.columnAtATimeReader(ctx);
                        try (BlockLoader.Block block = columnAtATimeReader.read(blockFactory, docs, start, false)) {
                            BytesRefBlock result = (BytesRefBlock) block;
                            BytesRef scratch = new BytesRef();
                            for (int i = 0; i < result.getPositionCount(); i++) {
                                counts.merge(result.getBytesRef(i, scratch).utf8ToString(), 1, (lhs, rhs) -> lhs + rhs);
                            }
                            boolean singleValue = true;
                            BytesRef first = BytesRef.deepCopyOf(result.getBytesRef(0, scratch));
                            for (int i = 1; i < result.getPositionCount(); i++) {
                                if (first.equals(result.getBytesRef(i, scratch)) == false) {
                                    singleValue = false;
                                    break;
                                }
                            }
                            BytesRefVector valuesVector = result.asVector();
                            assertNotNull(valuesVector);
                            assertThat(valuesVector.isConstant(), equalTo(singleValue));
                        }
                        start = end;
                    }
                }
            }
            assertMap(counts, matchesMap().entry("a", docCount).entry("b", docCount).entry("c", docCount).entry("d", docCount));
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

    public void testAllNull() throws IOException {
        BlockFactory factory = blockFactory();
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
                    try (
                        SingletonOrdinalsBuilder builder = new SingletonOrdinalsBuilder(factory, docValues, ctx.reader().numDocs(), false);
                    ) {
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
        BlockFactory factory = blockFactory();
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
                        var b1 = new SingletonOrdinalsBuilder(factory, ctx.reader().getSortedDocValues("f"), batchSize, randomBoolean());
                        var b2 = new SingletonOrdinalsBuilder(factory, ctx.reader().getSortedDocValues("f"), batchSize, randomBoolean())
                    ) {
                        for (int i = 0; i < batchSize; i++) {
                            appendOrd(b1, ord);
                            appendOrd(b2, ord);
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

    private void appendOrd(SingletonOrdinalsBuilder builder, int ord) {
        if (randomBoolean()) {
            builder.appendOrd(ord);
        } else if (randomBoolean()) {
            int prefix = between(0, 2);
            int suffix = between(0, 2);
            int[] ords = new int[prefix + 1 + suffix];
            ords[prefix] = ord;
            builder.appendOrds(ords, prefix, 1, ord, ord);
        } else {
            builder.appendOrds(ord, 1);
        }
    }

    public void testEmitConstantBlocks() throws IOException {
        var config = new IndexWriterConfig();
        config.setIndexSort(new Sort(new SortField("f", SortField.Type.STRING)));
        int totalDocs = between(100, 1000);
        var values = new BytesRef[] { new BytesRef("a"), new BytesRef("b"), new BytesRef("c"), new BytesRef("d") };
        int[] counts = new int[4];
        try (var directory = newDirectory(); var indexWriter = new IndexWriter(directory, config)) {
            for (int i = 0; i < totalDocs; i++) {
                int valueIndex = randomIntBetween(0, values.length - 1);
                counts[valueIndex]++;
                indexWriter.addDocument(List.of(new SortedDocValuesField("f", values[valueIndex])));
            }
            final int[] starts = new int[values.length + 1];
            for (int i = 0; i < values.length; i++) {
                starts[i + 1] = starts[i] + counts[i];
            }
            var keywordField = new KeywordFieldMapper.KeywordFieldType("f");
            var blockLoader = keywordField.blockLoader(ValuesSourceReaderOperatorTests.blContext());
            var blockFactory = new ComputeBlockLoaderFactory(blockFactory());
            try (IndexReader reader = DirectoryReader.open(indexWriter)) {
                for (LeafReaderContext ctx : reader.leaves()) {
                    int start = 0;
                    int numDocs = ctx.reader().numDocs();
                    while (start < numDocs) {
                        int end = start + randomIntBetween(1, numDocs - start);
                        BlockLoader.Docs docs = new BlockLoader.Docs() {
                            @Override
                            public int count() {
                                return end;
                            }

                            @Override
                            public int get(int i) {
                                return i;
                            }
                        };
                        var columnAtATimeReader = blockLoader.columnAtATimeReader(ctx);
                        try (BlockLoader.Block block = columnAtATimeReader.read(blockFactory, docs, start, false)) {
                            BytesRefBlock result = (BytesRefBlock) block;
                            assertNotNull(result.asVector());
                            boolean enclosedInSingleRange = false;
                            for (int i = 0; i < starts.length - 1; i++) {
                                if (starts[i] <= start && end <= starts[i + 1]) {
                                    enclosedInSingleRange = true;
                                    break;
                                }
                            }
                            assertThat(result.asVector().isConstant(), equalTo(enclosedInSingleRange));
                        }
                        start = end;
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
}
