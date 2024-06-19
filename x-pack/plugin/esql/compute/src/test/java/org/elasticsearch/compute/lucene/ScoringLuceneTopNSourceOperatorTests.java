/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntVectorBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AnyOperatorTestCase;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.OperatorTestCase;
import org.elasticsearch.compute.operator.TestResultPageSinkOperator;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.search.sort.SortBuilders;
import org.hamcrest.Matcher;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.matchesRegex;

public class ScoringLuceneTopNSourceOperatorTests extends AnyOperatorTestCase {
    private static final String FIELD_NAME = "text";
    private static final MappedFieldType TEXT_FIELD_TYPE = new TextFieldMapper.TextFieldType(FIELD_NAME, true, true, new HashMap<>());
    private final Directory directory = newDirectory();
    private IndexReader reader;

    @After
    public void closeIndex() throws IOException {
        IOUtils.close(reader, directory);
    }

    @Override
    protected LuceneTopNSourceOperator.Factory simple() {
        return simple(10_000, 100);
    }

    private ScoringLuceneTopNSourceOperator.Factory simple(int size, int limit) {
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
                doc.add(new TextField(FIELD_NAME, String.valueOf(d), Field.Store.YES));
                writer.addDocument(doc);
                if (d % commitEvery == 0) {
                    writer.commit();
                }
            }
            reader = writer.getReader();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        ShardContext ctx = new LuceneSourceOperatorTests.MockShardContext(reader, 0);
        Function<ShardContext, Query> queryFunction = c -> new MatchAllDocsQuery();
        int taskConcurrency = 0;
        int maxPageSize = between(10, Math.max(10, size));
        return new ScoringLuceneTopNSourceOperator.Factory(
            List.of(ctx),
            queryFunction,
            DataPartitioning.SHARD,
            taskConcurrency,
            maxPageSize,
            limit,
            List.of(SortBuilders.scoreSort())
        );
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return matchesRegex("ScoringLuceneTopNSourceOperator\\[maxPageSize = \\d+scoring]");
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return matchesRegex(
            "ScoringLuceneTopNSourceOperator\\[dataPartitioning = (DOC|SHARD|SEGMENT), maxPageSize = \\d+, limit = 100, sorts = \\[\\{.+}]]"
        );
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
        ScoringLuceneTopNSourceOperator.Factory factory = simple(size, limit);
        Operator.OperatorFactory readS = ValuesSourceReaderOperatorTests.factory(reader, TEXT_FIELD_TYPE, ElementType.BYTES_REF);

        List<Page> results = new ArrayList<>();
        OperatorTestCase.runDriver(
            new Driver(ctx, factory.get(ctx), List.of(readS.get(ctx)), new TestResultPageSinkOperator(results::add), () -> {})
        );
        OperatorTestCase.assertDriverContext(ctx);

        for (Page page : results) {
            IntVectorBlock sBlock = page.getBlock(1);
            for (int p = 0; p < page.getPositionCount(); p++) {
                assertThat(sBlock.getInt(sBlock.getFirstValueIndex(p)), greaterThan(0));
            }
        }
        int pages = (int) Math.ceil((float) Math.min(size, limit) / factory.maxPageSize());
        assertThat(results, hasSize(pages));
    }
}
