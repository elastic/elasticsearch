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
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AnyOperatorTestCase;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.OperatorTestCase;
import org.elasticsearch.compute.operator.TestResultPageSinkOperator;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NestedLookup;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.support.NestedScope;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LuceneTopNSourceOperatorTests extends AnyOperatorTestCase {
    private static final MappedFieldType S_FIELD = new NumberFieldMapper.NumberFieldType("s", NumberFieldMapper.NumberType.LONG);
    private Directory directory = newDirectory();
    private IndexReader reader;

    @After
    public void closeIndex() throws IOException {
        IOUtils.close(reader, directory);
    }

    @Override
    protected LuceneTopNSourceOperator.Factory simple(BigArrays bigArrays) {
        return simple(bigArrays, DataPartitioning.SHARD, 10_000, 100);
    }

    private LuceneTopNSourceOperator.Factory simple(BigArrays bigArrays, DataPartitioning dataPartitioning, int size, int limit) {
        int commitEvery = Math.max(1, size / 10);
        try (
            RandomIndexWriter writer = new RandomIndexWriter(
                random(),
                directory,
                newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE)
            )
        ) {
            for (int d = 0; d < size; d++) {
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

        SearchContext ctx = LuceneSourceOperatorTests.mockSearchContext(reader);
        SearchExecutionContext ectx = mock(SearchExecutionContext.class);
        when(ctx.getSearchExecutionContext()).thenReturn(ectx);
        when(ectx.getFieldType(anyString())).thenAnswer(inv -> {
            String name = inv.getArgument(0);
            return switch (name) {
                case "s" -> S_FIELD;
                default -> throw new IllegalArgumentException("don't support [" + name + "]");
            };
        });
        when(ectx.getForField(any(), any())).thenAnswer(inv -> {
            MappedFieldType ft = inv.getArgument(0);
            IndexFieldData.Builder builder = ft.fielddataBuilder(FieldDataContext.noRuntimeFields("test"));
            return builder.build(new IndexFieldDataCache.None(), bigArrays.breakerService());
        });
        when(ectx.nestedScope()).thenReturn(new NestedScope());
        when(ectx.nestedLookup()).thenReturn(NestedLookup.EMPTY);
        when(ectx.getIndexReader()).thenReturn(reader);
        Function<SearchContext, Query> queryFunction = c -> new MatchAllDocsQuery();
        int taskConcurrency = 0;
        int maxPageSize = between(10, Math.max(10, size));
        List<SortBuilder<?>> sorts = List.of(new FieldSortBuilder("s"));
        return new LuceneTopNSourceOperator.Factory(
            List.of(ctx),
            queryFunction,
            dataPartitioning,
            taskConcurrency,
            maxPageSize,
            limit,
            sorts
        );
    }

    @Override
    protected String expectedToStringOfSimple() {
        assumeFalse("can't support variable maxPageSize", true); // TODO allow testing this
        return "LuceneTopNSourceOperator[shardId=0, maxPageSize=**random**]";
    }

    @Override
    protected String expectedDescriptionOfSimple() {
        assumeFalse("can't support variable maxPageSize", true); // TODO allow testing this
        return """
            LuceneTopNSourceOperator[dataPartitioning = SHARD, maxPageSize = **random**, limit = 100, sorts = [{"s":{"order":"asc"}}]]""";
    }

    // TODO tests for the other data partitioning configurations

    public void testShardDataPartitioning() {
        testShardDataPartitioning(driverContext());
    }

    public void testShardDataPartitioningWithCranky() {
        try {
            testShardDataPartitioning(crankyDriverContext());
            logger.info("cranky didn't break");
        } catch (CircuitBreakingException e) {
            logger.info("broken", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    private void testShardDataPartitioning(DriverContext context) {
        int size = between(1_000, 20_000);
        int limit = between(10, size);
        testSimple(context, size, limit);
    }

    public void testEmpty() {
        testEmpty(driverContext());
    }

    public void testEmptyWithCranky() {
        try {
            testEmpty(crankyDriverContext());
            logger.info("cranky didn't break");
        } catch (CircuitBreakingException e) {
            logger.info("broken", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    private void testEmpty(DriverContext context) {
        testSimple(context, 0, between(10, 10_000));
    }

    private void testSimple(DriverContext ctx, int size, int limit) {
        LuceneTopNSourceOperator.Factory factory = simple(ctx.bigArrays(), DataPartitioning.SHARD, size, limit);
        Operator.OperatorFactory readS = ValuesSourceReaderOperatorTests.factory(reader, S_FIELD);

        List<Page> results = new ArrayList<>();
        OperatorTestCase.runDriver(
            new Driver(ctx, factory.get(ctx), List.of(readS.get(ctx)), new TestResultPageSinkOperator(results::add), () -> {})
        );
        OperatorTestCase.assertDriverContext(ctx);

        long expectedS = 0;
        for (Page page : results) {
            if (limit - expectedS < factory.maxPageSize()) {
                assertThat(page.getPositionCount(), equalTo((int) (limit - expectedS)));
            } else {
                assertThat(page.getPositionCount(), equalTo(factory.maxPageSize()));
            }
            LongBlock sBlock = page.getBlock(1);
            for (int p = 0; p < page.getPositionCount(); p++) {
                assertThat(sBlock.getLong(sBlock.getFirstValueIndex(p)), equalTo(expectedS++));
            }
        }
        int pages = (int) Math.ceil((float) Math.min(size, limit) / factory.maxPageSize());
        assertThat(results, hasSize(pages));
    }
}
