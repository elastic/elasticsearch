/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CannedSourceOperator;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.OperatorTestCase;
import org.elasticsearch.compute.operator.PageConsumerOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.FieldContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class ValuesSourceReaderOperatorTests extends OperatorTestCase {
    private Directory directory = newDirectory();
    private IndexReader reader;

    @After
    public void closeIndex() throws IOException {
        IOUtils.close(reader, directory);
    }

    @Override
    protected Operator.OperatorFactory simple(BigArrays bigArrays) {
        return factory(new NumberFieldMapper.NumberFieldType("long", NumberFieldMapper.NumberType.LONG));
    }

    private Operator.OperatorFactory factory(MappedFieldType ft) {
        IndexFieldData<?> fd = ft.fielddataBuilder(FieldDataContext.noRuntimeFields("test"))
            .build(new IndexFieldDataCache.None(), new NoneCircuitBreakerService());
        FieldContext fc = new FieldContext(ft.name(), fd, ft);
        ValuesSource vs = CoreValuesSourceType.NUMERIC.getField(fc, null);
        return new ValuesSourceReaderOperator.ValuesSourceReaderOperatorFactory(
            List.of(new ValueSourceInfo(CoreValuesSourceType.NUMERIC, vs, ElementType.LONG, reader)),
            0,
            ft.name()
        );
    }

    @Override
    protected SourceOperator simpleInput(int size) {
        // The test wants more than one segment. We short for about 10.
        int commitEvery = Math.max(1, size / 10);
        try (
            RandomIndexWriter writer = new RandomIndexWriter(
                random(),
                directory,
                newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE)
            )
        ) {
            for (int d = 0; d < size; d++) {
                writer.addDocument(List.of(new SortedNumericDocValuesField("key", d), new SortedNumericDocValuesField("long", d)));
                if (d % commitEvery == 0) {
                    writer.commit();
                }
            }
            reader = writer.getReader();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new LuceneSourceOperator(reader, 0, new MatchAllDocsQuery());
    }

    @Override
    protected String expectedDescriptionOfSimple() {
        return "ValuesSourceReaderOperator(field = long)";
    }

    @Override
    protected String expectedToStringOfSimple() {
        return "ValuesSourceReaderOperator";
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        long expectedSum = 0;
        long current = 0;

        long sum = 0;
        for (Page r : results) {
            LongBlock b = r.getBlock(r.getBlockCount() - 1);
            for (int p = 0; p < b.getPositionCount(); p++) {
                expectedSum += current;
                current++;
                sum += b.getLong(p);
            }
        }

        assertThat(sum, equalTo(expectedSum));
    }

    @Override
    protected ByteSizeValue smallEnoughToCircuitBreak() {
        assumeTrue("doesn't use big arrays so can't break", false);
        return null;
    }

    public void testLoadFromManyPagesAtOnce() {
        loadFromManyPagesAtOnce(false);
    }

    public void testLoadFromManyPagesShuffled() {
        loadFromManyPagesAtOnce(true);
    }

    private void loadFromManyPagesAtOnce(boolean shuffle) {
        Page source = CannedSourceOperator.mergePages(
            CannedSourceOperator.collectPages(simpleInput(between(1_000, 10 * LuceneSourceOperator.PAGE_SIZE)))
        );

        if (shuffle) {
            List<Integer> shuffleList = new ArrayList<>();
            IntStream.range(0, source.getPositionCount()).forEach(i -> shuffleList.add(i));
            Randomness.shuffle(shuffleList);
            int[] shuffleArray = shuffleList.stream().mapToInt(Integer::intValue).toArray();
            Block[] shuffledBlocks = new Block[source.getBlockCount()];
            for (int b = 0; b < shuffledBlocks.length; b++) {
                shuffledBlocks[b] = source.getBlock(b).filter(shuffleArray);
            }
            source = new Page(shuffledBlocks);
        }

        List<Page> results = new ArrayList<>();
        try (
            Driver d = new Driver(
                new CannedSourceOperator(List.of(source).iterator()),
                List.of(
                    factory(new NumberFieldMapper.NumberFieldType("key", NumberFieldMapper.NumberType.LONG)).get(),
                    factory(new NumberFieldMapper.NumberFieldType("long", NumberFieldMapper.NumberType.LONG)).get()
                ),
                new PageConsumerOperator(page -> results.add(page)),
                () -> {}
            )
        ) {
            d.run();
        }
        assertThat(results, hasSize(1));
        for (Page p : results) {
            LongVector keys = p.<LongBlock>getBlock(1).asVector();
            LongVector longs = p.<LongBlock>getBlock(2).asVector();
            for (int i = 0; i < p.getPositionCount(); i++) {
                assertThat(longs.getLong(i), equalTo(keys.getLong(i)));
            }
        }
    }
}
