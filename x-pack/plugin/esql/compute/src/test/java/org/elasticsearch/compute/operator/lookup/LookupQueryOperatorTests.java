/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.lookup;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.tests.store.MockDirectoryWrapper;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromSingleton;
import org.elasticsearch.compute.lucene.LuceneSourceOperatorTests;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.compute.operator.WarningsTests.TestWarningsSource;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.search.internal.AliasFilter;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link LookupQueryOperator}.
 */
public class LookupQueryOperatorTests extends OperatorTestCase {

    private DirectoryData directoryData;

    @Before
    public void setupDirectory() throws IOException {
        // Create a directory with terms for testing
        // Each document has a term like "term-0", "term-1", etc.
        List<List<String>> terms = IntStream.range(0, 100).mapToObj(i -> List.of("term-" + i)).toList();
        directoryData = makeDirectoryWith(terms);
    }

    @After
    public void closeDirectory() throws IOException {
        if (directoryData != null) {
            directoryData.close();
        }
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        // Generate input terms that will match documents in the index
        // Each position gets a term like "term-0", "term-1", etc.
        return new SourceOperator() {
            private int position = 0;

            @Override
            public void finish() {}

            @Override
            public boolean isFinished() {
                return position >= size;
            }

            @Override
            public Page getOutput() {
                if (position >= size) {
                    return null;
                }
                int pageSize = Math.min(randomIntBetween(1, 100), size - position);
                try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(pageSize)) {
                    for (int i = 0; i < pageSize; i++) {
                        // Use modulo to wrap around if size > 100
                        String term = "term-" + ((position + i) % 100);
                        builder.appendBytesRef(new BytesRef(term));
                    }
                    position += pageSize;
                    return new Page(builder.build());
                }
            }

            @Override
            public void close() {}
        };
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return new Operator.OperatorFactory() {
            @Override
            public Operator get(DriverContext driverContext) {
                QueryList queryList = QueryList.rawTermQueryList(directoryData.field, AliasFilter.EMPTY, 0, ElementType.BYTES_REF);
                return new LookupQueryOperator(
                    driverContext.blockFactory(),
                    LookupQueryOperator.DEFAULT_MAX_PAGE_SIZE,
                    queryList,
                    BlockOptimization.NONE,
                    new IndexedByShardIdFromSingleton<>(new LuceneSourceOperatorTests.MockShardContext(directoryData.reader)),
                    0,
                    directoryData.searchExecutionContext,
                    warnings()
                );
            }

            @Override
            public String describe() {
                return "LookupQueryOperator[maxPageSize=256]";
            }
        };
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("LookupQueryOperator[maxPageSize=256]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo("LookupQueryOperator[maxPageSize=256]");
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        // Count total input positions
        int totalInputPositions = input.stream().mapToInt(Page::getPositionCount).sum();

        // Each input position should produce exactly one match (since each term matches one doc)
        int totalOutputPositions = results.stream().mapToInt(Page::getPositionCount).sum();
        assertThat(totalOutputPositions, equalTo(totalInputPositions));

        // Verify the output structure: should have DocBlock and positions IntBlock
        for (Page page : results) {
            assertThat(page.getBlockCount(), equalTo(2));
            assertTrue(page.getBlock(0) instanceof DocBlock);
            assertTrue(page.getBlock(1) instanceof IntBlock);
        }
    }

    /**
     * Test the critical scenario: when all queries produce NO matches,
     * getOutput() should return null AND canProduceMoreDataWithoutExtraInput() should return false.
     */
    public void testNoMatchesScenario() throws Exception {
        // Create a directory with terms that won't match the input
        try (DirectoryData noMatchDirectory = makeDirectoryWith(List.of(List.of("existing-term-1"), List.of("existing-term-2")))) {
            QueryList queryList = QueryList.rawTermQueryList(noMatchDirectory.field, AliasFilter.EMPTY, 0, ElementType.BYTES_REF);

            DriverContext driverContext = driverContext();
            try (
                LookupQueryOperator operator = new LookupQueryOperator(
                    driverContext.blockFactory(),
                    128,
                    queryList,
                    BlockOptimization.NONE,
                    new IndexedByShardIdFromSingleton<>(new LuceneSourceOperatorTests.MockShardContext(noMatchDirectory.reader)),
                    0,
                    noMatchDirectory.searchExecutionContext,
                    warnings()
                )
            ) {
                // Create input with non-matching terms
                try (BytesRefBlock.Builder builder = driverContext.blockFactory().newBytesRefBlockBuilder(5)) {
                    builder.appendBytesRef(new BytesRef("non-matching-1"));
                    builder.appendBytesRef(new BytesRef("non-matching-2"));
                    builder.appendBytesRef(new BytesRef("non-matching-3"));
                    builder.appendNull();
                    builder.appendBytesRef(new BytesRef("non-matching-4"));
                    Page inputPage = new Page(builder.build());

                    // Initially should not be able to produce data (no input yet)
                    assertFalse("Should not produce data without input", operator.canProduceMoreDataWithoutExtraInput());

                    // Add input
                    operator.addInput(inputPage);

                    // After adding input, should be able to produce more data
                    assertTrue("Should be able to produce data after adding input", operator.canProduceMoreDataWithoutExtraInput());

                    // Get output - should return null since no matches
                    Page output = operator.getOutput();
                    assertNull("Should return null when no matches", output);

                    // CRITICAL: After returning null, canProduceMoreDataWithoutExtraInput should be false
                    assertFalse(
                        "canProduceMoreDataWithoutExtraInput should return false after all queries processed with no matches",
                        operator.canProduceMoreDataWithoutExtraInput()
                    );

                    // Should need new input now
                    assertTrue("Should need input after input page consumed", operator.needsInput());

                    operator.finish();
                    assertTrue("Operator should be finished", operator.isFinished());
                }
            }
        }
    }

    /**
     * Test that getOutput never returns null while canProduceMoreDataWithoutExtraInput returns true.
     * This is the invariant that was potentially violated in the hang scenario.
     */
    public void testGetOutputNeverNullWhileCanProduceMore() throws Exception {
        DriverContext driverContext = driverContext();
        QueryList queryList = QueryList.rawTermQueryList(directoryData.field, AliasFilter.EMPTY, 0, ElementType.BYTES_REF);

        try (
            LookupQueryOperator operator = new LookupQueryOperator(
                driverContext.blockFactory(),
                10, // Small page size to produce multiple pages
                queryList,
                BlockOptimization.NONE,
                new IndexedByShardIdFromSingleton<>(new LuceneSourceOperatorTests.MockShardContext(directoryData.reader)),
                0,
                directoryData.searchExecutionContext,
                warnings()
            )
        ) {
            // Create input with many matching terms
            try (BytesRefBlock.Builder builder = driverContext.blockFactory().newBytesRefBlockBuilder(50)) {
                for (int i = 0; i < 50; i++) {
                    builder.appendBytesRef(new BytesRef("term-" + i));
                }
                Page inputPage = new Page(builder.build());

                operator.addInput(inputPage);

                int iterations = 0;
                int maxIterations = 100; // Safety limit

                while (operator.canProduceMoreDataWithoutExtraInput() && iterations < maxIterations) {
                    Page output = operator.getOutput();
                    // This is the key invariant: if canProduceMoreDataWithoutExtraInput() returns true,
                    // getOutput() should NOT return null
                    assertNotNull("getOutput() should not return null when canProduceMoreDataWithoutExtraInput() is true", output);
                    output.releaseBlocks();
                    iterations++;
                }

                assertThat("Should have produced multiple pages", iterations, org.hamcrest.Matchers.greaterThan(1));

                operator.finish();
            }
        }
    }

    /**
     * Test mixed matches and no-matches scenario.
     */
    public void testMixedMatchesAndNoMatches() throws Exception {
        DriverContext driverContext = driverContext();
        QueryList queryList = QueryList.rawTermQueryList(directoryData.field, AliasFilter.EMPTY, 0, ElementType.BYTES_REF);

        try (
            LookupQueryOperator operator = new LookupQueryOperator(
                driverContext.blockFactory(),
                128,
                queryList,
                BlockOptimization.NONE,
                new IndexedByShardIdFromSingleton<>(new LuceneSourceOperatorTests.MockShardContext(directoryData.reader)),
                0,
                directoryData.searchExecutionContext,
                warnings()
            )
        ) {
            // Mix of matching and non-matching terms
            try (BytesRefBlock.Builder builder = driverContext.blockFactory().newBytesRefBlockBuilder(7)) {
                builder.appendBytesRef(new BytesRef("term-0"));       // matches
                builder.appendBytesRef(new BytesRef("no-match-1"));   // no match
                builder.appendBytesRef(new BytesRef("term-1"));       // matches
                builder.appendBytesRef(new BytesRef("no-match-2"));   // no match
                builder.appendNull();                                  // null
                builder.appendBytesRef(new BytesRef("term-2"));       // matches
                builder.appendBytesRef(new BytesRef("no-match-3"));   // no match
                Page inputPage = new Page(builder.build());

                operator.addInput(inputPage);

                Page output = operator.getOutput();
                assertNotNull("Should have matches", output);

                // Should have 3 matches (positions 0, 2, 5)
                assertThat(output.getPositionCount(), equalTo(3));

                IntBlock positions = output.getBlock(1);
                Set<Integer> matchedPositions = new HashSet<>();
                for (int i = 0; i < output.getPositionCount(); i++) {
                    matchedPositions.add(positions.getInt(i));
                }
                assertThat(matchedPositions, equalTo(Set.of(0, 2, 5)));

                output.releaseBlocks();

                // After processing, should not produce more data
                assertFalse(
                    "canProduceMoreDataWithoutExtraInput should return false after processing",
                    operator.canProduceMoreDataWithoutExtraInput()
                );

                operator.finish();
            }
        }
    }

    private static Warnings warnings() {
        return Warnings.createWarnings(DriverContext.WarningsMode.COLLECT, new TestWarningsSource("test"));
    }

    private record DirectoryData(
        DirectoryReader reader,
        MockDirectoryWrapper dir,
        SearchExecutionContext searchExecutionContext,
        MappedFieldType field
    ) implements AutoCloseable {
        @Override
        public void close() throws IOException {
            IOUtils.close(reader, dir);
        }
    }

    private static DirectoryData makeDirectoryWith(List<List<String>> terms) throws IOException {
        MockDirectoryWrapper dir = newMockDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig();
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        try (IndexWriter writer = new IndexWriter(dir, iwc)) {
            for (var termList : terms) {
                Document doc = new Document();
                for (String term : termList) {
                    doc.add(new KeywordField("uid", term, Field.Store.NO));
                }
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
            writer.commit();

            var directoryReader = DirectoryReader.open(writer);
            var indexSearcher = newSearcher(directoryReader);
            var searchExecutionContext = mock(SearchExecutionContext.class);
            var field = new KeywordFieldMapper.KeywordFieldType("uid");
            var fieldDataContext = FieldDataContext.noRuntimeFields("index", "test");
            var indexFieldData = field.fielddataBuilder(fieldDataContext)
                .build(new IndexFieldDataCache.None(), new NoneCircuitBreakerService());

            when(searchExecutionContext.searcher()).thenReturn(indexSearcher);
            when(searchExecutionContext.getForField(field, MappedFieldType.FielddataOperation.SEARCH)).thenReturn(indexFieldData);

            return new DirectoryData(directoryReader, dir, searchExecutionContext, field);
        }
    }
}
