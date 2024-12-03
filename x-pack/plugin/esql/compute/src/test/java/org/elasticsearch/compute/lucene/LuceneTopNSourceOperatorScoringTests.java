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
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.OperatorTestCase;
import org.elasticsearch.compute.operator.TestResultPageSinkOperator;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.search.sort.SortBuilder;
import org.hamcrest.Matcher;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.matchesRegex;

public class LuceneTopNSourceOperatorScoringTests extends LuceneTopNSourceOperatorTests {
    private static final MappedFieldType S_FIELD = new NumberFieldMapper.NumberFieldType("s", NumberFieldMapper.NumberType.LONG);
    private Directory directory = newDirectory();
    private IndexReader reader;

    @After
    private void closeIndex() throws IOException {
        IOUtils.close(reader, directory);
    }

    @Override
    protected LuceneTopNSourceOperator.Factory simple() {
        return simple(DataPartitioning.SHARD, 10_000, 100);
    }

    private LuceneTopNSourceOperator.Factory simple(DataPartitioning dataPartitioning, int size, int limit) {
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

        ShardContext ctx = new LuceneSourceOperatorTests.MockShardContext(reader, 0) {
            @Override
            public Optional<SortAndFormats> buildSort(List<SortBuilder<?>> sorts) {
                SortField field = new SortedNumericSortField("s", SortField.Type.LONG, false, SortedNumericSelector.Type.MIN);
                return Optional.of(new SortAndFormats(new Sort(field), new DocValueFormat[] { null }));
            }
        };
        Function<ShardContext, Query> queryFunction = c -> new MatchAllDocsQuery();
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
            sorts,
            true // scoring
        );
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return matchesRegex("LuceneTopNSourceOperator\\[maxPageSize = \\d+, limit = 100, scoreMode = COMPLETE, sorts = \\[\\{.+}]]");
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return matchesRegex(
            "LuceneTopNSourceOperator"
                + "\\[dataPartitioning = (DOC|SHARD|SEGMENT), maxPageSize = \\d+, limit = 100, scoreMode = COMPLETE, sorts = \\[\\{.+}]]"
        );
    }

    @Override
    protected void testSimple(DriverContext ctx, int size, int limit) {
        LuceneTopNSourceOperator.Factory factory = simple(DataPartitioning.SHARD, size, limit);
        Operator.OperatorFactory readS = ValuesSourceReaderOperatorTests.factory(reader, S_FIELD, ElementType.LONG);

        List<Page> results = new ArrayList<>();
        OperatorTestCase.runDriver(
            new Driver(ctx, factory.get(ctx), List.of(readS.get(ctx)), new TestResultPageSinkOperator(results::add), () -> {})
        );
        OperatorTestCase.assertDriverContext(ctx);

        long expectedS = 0;
        int maxPageSize = factory.maxPageSize();
        for (Page page : results) {
            if (limit - expectedS < maxPageSize) {
                assertThat(page.getPositionCount(), equalTo((int) (limit - expectedS)));
            } else {
                assertThat(page.getPositionCount(), equalTo(maxPageSize));
            }
            DoubleBlock sBlock = page.getBlock(1);
            for (int p = 0; p < page.getPositionCount(); p++) {
                assertThat(sBlock.getDouble(sBlock.getFirstValueIndex(p)), equalTo(1.0d));
                expectedS++;
            }
        }
        int pages = (int) Math.ceil((float) Math.min(size, limit) / maxPageSize);
        assertThat(results, hasSize(pages));
    }
}
