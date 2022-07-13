/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.aggregations.metrics.GeoBoundsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.InternalGeoBounds;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.spatial.index.mapper.GeoShapeWithDocValuesFieldMapper;
import org.elasticsearch.xpack.sql.action.compute.Driver;
import org.elasticsearch.xpack.sql.action.compute.LongBlock;
import org.elasticsearch.xpack.sql.action.compute.LongGroupingOperator;
import org.elasticsearch.xpack.sql.action.compute.LongMaxOperator;
import org.elasticsearch.xpack.sql.action.compute.LongTransformer;
import org.elasticsearch.xpack.sql.action.compute.LucenePageCollector;
import org.elasticsearch.xpack.sql.action.compute.NumericDocValuesExtractor;
import org.elasticsearch.xpack.sql.action.compute.Operator;
import org.elasticsearch.xpack.sql.action.compute.Page;
import org.elasticsearch.xpack.sql.action.compute.PageConsumerOperator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;

public class OperatorTests extends ESTestCase {

    class RandomLongBlockSourceOperator implements Operator {

        boolean finished;

        @Override
        public Page getOutput() {
            if (random().nextInt(100) < 1) {
                finish();
            }
            final int size = randomIntBetween(1, 10);
            final long[] array = new long[size];
            for (int i = 0; i < array.length; i++) {
                array[i] = randomLongBetween(0, 5);
            }
            return new Page(new LongBlock(array, array.length));
        }

        @Override
        public boolean isFinished() {
            return finished;
        }

        @Override
        public void finish() {
            finished = true;
        }

        @Override
        public boolean needsInput() {
            return false;
        }

        @Override
        public void addInput(Page page) {
            throw new UnsupportedOperationException();
        }
    }

    public void testOperators() {
        Driver driver = new Driver(List.of(
            new RandomLongBlockSourceOperator(),
            new LongTransformer(0, i -> i + 1),
            new LongGroupingOperator(1, BigArrays.NON_RECYCLING_INSTANCE),
            new LongMaxOperator(2),
            new PageConsumerOperator(page -> logger.info("New block: {}", page))),
            () -> {});
        driver.run();
    }

    public void testOperatorsWithLucene() throws IOException, InterruptedException {
        int numDocs = 100000;
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            Document doc = new Document();
            NumericDocValuesField docValuesField = new NumericDocValuesField("value", 0);
            for (int i = 0; i < numDocs; i++) {
                doc.clear();
                docValuesField.setLongValue(i);
                doc.add(docValuesField);
                w.addDocument(doc);
            }
            w.commit();

            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                LucenePageCollector pageCollector = new LucenePageCollector();
                Thread t = new Thread(() -> {
                    logger.info("Start processing");
                    try {
                        searcher.search(new MatchAllDocsQuery(), pageCollector);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                    pageCollector.finish();
                });
                t.start();
                AtomicInteger pageCount = new AtomicInteger();
                AtomicInteger rowCount = new AtomicInteger();
                AtomicReference<Page> lastPage = new AtomicReference<>();

                // implements cardinality on value field
                Driver driver = new Driver(List.of(
                    pageCollector,
                    new NumericDocValuesExtractor(searcher.getIndexReader(), 0, 1, "value"),
                    new LongGroupingOperator(2, BigArrays.NON_RECYCLING_INSTANCE),
                    new LongMaxOperator(3), // returns highest group number
                    new LongTransformer(0, i -> i + 1), // adds +1 to group number (which start with 0) to get group count
                    new PageConsumerOperator(page -> {
                        logger.info("New block: {}", page);
                        pageCount.incrementAndGet();
                        rowCount.addAndGet(page.getPositionCount());
                        lastPage.set(page);
                    })),
                    () -> {});
                driver.run();
                t.join();
                assertEquals(1, pageCount.get());
                assertEquals(1, rowCount.get());
                assertEquals(numDocs, lastPage.get().getBlock(1).getLong(0));
            }
        }
    }

    // Operator that just chains blocks through, but allows checking some conditions
    public static class AssertOperator implements Operator {



        @Override
        public Page getOutput() {
            return null;
        }

        @Override
        public boolean isFinished() {
            return false;
        }

        @Override
        public void finish() {

        }

        @Override
        public boolean needsInput() {
            return false;
        }

        @Override
        public void addInput(Page page) {

        }
    }
}
