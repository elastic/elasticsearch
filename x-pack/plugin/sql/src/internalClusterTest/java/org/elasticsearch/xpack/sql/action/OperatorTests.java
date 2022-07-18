/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.test.ESTestCase;
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
import org.elasticsearch.xpack.sql.action.compute.exchange.ExchangeSink;
import org.elasticsearch.xpack.sql.action.compute.exchange.ExchangeSinkOperator;
import org.elasticsearch.xpack.sql.action.compute.exchange.ExchangeSource;
import org.elasticsearch.xpack.sql.action.compute.exchange.ExchangeSourceOperator;
import org.elasticsearch.xpack.sql.action.compute.exchange.PassthroughExchanger;
import org.elasticsearch.xpack.sql.action.compute.exchange.RandomExchanger;
import org.elasticsearch.xpack.sql.action.compute.exchange.RandomUnionSourceOperator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

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

        @Override
        public void close() {

        }
    }

    public void testOperators() {
        Driver driver = new Driver(List.of(
            new RandomLongBlockSourceOperator(),
            new LongTransformer(0, i -> i + 1),
            new LongGroupingOperator(1, BigArrays.NON_RECYCLING_INSTANCE),
            new LongMaxOperator(2),
            new PageConsumerOperator(page -> logger.info("New page: {}", page))),
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
                        logger.info("New page: {}", page);
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

    public void testOperatorsWithPassthroughExchange() throws InterruptedException {
        ExchangeSource exchangeSource = new ExchangeSource();
        Consumer<ExchangeSink> sinkFinished = sink -> {
            exchangeSource.finish();
        };
        Driver driver1 = new Driver(List.of(
            new RandomLongBlockSourceOperator(),
            new LongTransformer(0, i -> i + 1),
            new LongGroupingOperator(1, BigArrays.NON_RECYCLING_INSTANCE),
            new ExchangeSinkOperator(new ExchangeSink(new PassthroughExchanger(exchangeSource), sinkFinished))),
            () -> {});

        Driver driver2 = new Driver(List.of(
            new ExchangeSourceOperator(exchangeSource),
            new PageConsumerOperator(page -> logger.info("New page: {}", page))),
            () -> {});

        Thread t1 = new Thread(driver1::run);
        Thread t2 = new Thread(driver2::run);
        t1.start();
        t2.start();
        t1.join();
        t2.join();
    }

    public void testOperatorsWithRandomExchange() throws InterruptedException {
        ExchangeSource exchangeSource1 = new ExchangeSource();
        ExchangeSource exchangeSource2 = new ExchangeSource();

        Consumer<ExchangeSink> sink1Finished = sink -> {
            exchangeSource1.finish();
            exchangeSource2.finish();
        };

        ExchangeSource exchangeSource3 = new ExchangeSource();
        ExchangeSource exchangeSource4 = new ExchangeSource();

        Driver driver1 = new Driver(List.of(
            new RandomLongBlockSourceOperator(),
            new LongTransformer(0, i -> i + 1),
            new ExchangeSinkOperator(new ExchangeSink(new RandomExchanger(List.of(exchangeSource1::addPage, exchangeSource2::addPage)),
                sink1Finished))),
            () -> {});

        Driver driver2 = new Driver(List.of(
            new ExchangeSourceOperator(exchangeSource1),
            new LongGroupingOperator(1, BigArrays.NON_RECYCLING_INSTANCE),
            new ExchangeSinkOperator(new ExchangeSink(new PassthroughExchanger(exchangeSource3), s -> exchangeSource3.finish()))),
            () -> {});

        Driver driver3 = new Driver(List.of(
            new ExchangeSourceOperator(exchangeSource2),
            new LongMaxOperator(1),
            new ExchangeSinkOperator(new ExchangeSink(new PassthroughExchanger(exchangeSource4), s -> exchangeSource4.finish()))),
            () -> {});

        Driver driver4 = new Driver(List.of(
            new RandomUnionSourceOperator(List.of(exchangeSource3, exchangeSource4)),
            new PageConsumerOperator(page -> logger.info("New page with #blocks: {}", page.getBlockCount()))),
            () -> {});

        Thread t1 = new Thread(driver1::run);
        Thread t2 = new Thread(driver2::run);
        Thread t3 = new Thread(driver3::run);
        Thread t4 = new Thread(driver4::run);
        t1.start();
        t2.start();
        t3.start();
        t4.start();
        t1.join();
        t2.join();
        t3.join();
        t4.join();
    }
}
