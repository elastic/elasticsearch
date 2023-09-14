/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AnyOperatorTestCase;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.OperatorTestCase;
import org.elasticsearch.compute.operator.PageConsumerOperator;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.cache.query.TrivialQueryCachingPolicy;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.SearchContext;
import org.junit.After;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LuceneCountOperatorTests extends AnyOperatorTestCase {
    private Directory directory = newDirectory();
    private IndexReader reader;

    @After
    public void closeIndex() throws IOException {
        IOUtils.close(reader, directory);
    }

    @Override
    protected LuceneCountOperator.Factory simple(BigArrays bigArrays) {
        return simple(bigArrays, randomFrom(DataPartitioning.values()), between(1, 10_000), 100);
    }

    private LuceneCountOperator.Factory simple(BigArrays bigArrays, DataPartitioning dataPartitioning, int numDocs, int limit) {
        int commitEvery = Math.max(1, numDocs / 10);
        try (
            RandomIndexWriter writer = new RandomIndexWriter(
                random(),
                directory,
                newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE)
            )
        ) {
            for (int d = 0; d < numDocs; d++) {
                List<IndexableField> doc = new ArrayList<>();
                doc.add(new SortedNumericDocValuesField("s", d));
                writer.addDocument(doc);
                if (d % commitEvery == 0) {
                    writer.commit();
                }
            }
            reader = writer.getReader();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        SearchContext ctx = mockSearchContext(reader);
        SearchExecutionContext ectx = mock(SearchExecutionContext.class);
        when(ctx.getSearchExecutionContext()).thenReturn(ectx);
        when(ectx.getIndexReader()).thenReturn(reader);
        Function<SearchContext, Query> queryFunction = c -> new MatchAllDocsQuery();
        return new LuceneCountOperator.Factory(List.of(ctx), queryFunction, dataPartitioning, 1, limit);
    }

    @Override
    protected String expectedToStringOfSimple() {
        assumeFalse("can't support variable maxPageSize", true); // TODO allow testing this
        return "LuceneCountOperator[shardId=0, maxPageSize=**random**]";
    }

    @Override
    protected String expectedDescriptionOfSimple() {
        assumeFalse("can't support variable maxPageSize", true); // TODO allow testing this
        return """
            LuceneCountOperator[dataPartitioning = SHARD, maxPageSize = **random**, limit = 100, sorts = [{"s":{"order":"asc"}}]]""";
    }

    // TODO tests for the other data partitioning configurations

    public void testShardDataPartitioning() {
        int size = between(1_000, 20_000);
        int limit = between(10, size);
        testSimple(size, limit);
    }

    public void testEmpty() {
        testSimple(0, between(10, 10_000));
    }

    private void testSimple(int size, int limit) {
        DriverContext ctx = new DriverContext();
        LuceneCountOperator.Factory factory = simple(nonBreakingBigArrays(), DataPartitioning.SHARD, size, limit);

        List<Page> results = new ArrayList<>();
        OperatorTestCase.runDriver(
            new Driver(ctx, factory.get(ctx), List.of(), new PageConsumerOperator(page -> results.add(page)), () -> {})
        );
        OperatorTestCase.assertDriverContext(ctx);

        for (Page page : results) {
            assertThat(page.getPositionCount(), is(1));
        }

        for (Page page : results) {
            LongBlock sBlock = page.getBlock(1);
            for (int p = 0; p < page.getPositionCount(); p++) {
                assertThat(sBlock.getLong(sBlock.getFirstValueIndex(p)), both(greaterThanOrEqualTo(0L)).and(lessThan((long) size)));
            }
        }
        assertThat(results, hasSize(1));
    }

    /**
     * Creates a mock search context with the given index reader.
     * The returned mock search context can be used to test with {@link LuceneOperator}.
     */
    public static SearchContext mockSearchContext(IndexReader reader) {
        try {
            ContextIndexSearcher searcher = new ContextIndexSearcher(
                reader,
                IndexSearcher.getDefaultSimilarity(),
                IndexSearcher.getDefaultQueryCache(),
                TrivialQueryCachingPolicy.NEVER,
                true
            );
            SearchContext searchContext = mock(SearchContext.class);
            when(searchContext.searcher()).thenReturn(searcher);
            return searchContext;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
