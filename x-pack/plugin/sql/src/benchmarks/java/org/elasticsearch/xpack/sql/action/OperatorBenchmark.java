/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.node.Node;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.sql.action.compute.data.Block;
import org.elasticsearch.xpack.sql.action.compute.data.LongBlock;
import org.elasticsearch.xpack.sql.action.compute.data.Page;
import org.elasticsearch.xpack.sql.action.compute.lucene.LuceneSourceOperator;
import org.elasticsearch.xpack.sql.action.compute.lucene.NumericDocValuesExtractor;
import org.elasticsearch.xpack.sql.action.compute.operator.Driver;
import org.elasticsearch.xpack.sql.action.compute.operator.LongAvgOperator;
import org.elasticsearch.xpack.sql.action.compute.operator.LongGroupingOperator;
import org.elasticsearch.xpack.sql.action.compute.operator.LongMaxOperator;
import org.elasticsearch.xpack.sql.action.compute.operator.LongTransformerOperator;
import org.elasticsearch.xpack.sql.action.compute.operator.Operator;
import org.elasticsearch.xpack.sql.action.compute.operator.PageConsumerOperator;
import org.elasticsearch.xpack.sql.action.compute.operator.exchange.ExchangeSink;
import org.elasticsearch.xpack.sql.action.compute.operator.exchange.ExchangeSinkOperator;
import org.elasticsearch.xpack.sql.action.compute.operator.exchange.ExchangeSource;
import org.elasticsearch.xpack.sql.action.compute.operator.exchange.ExchangeSourceOperator;
import org.elasticsearch.xpack.sql.action.compute.operator.exchange.PassthroughExchanger;
import org.elasticsearch.xpack.sql.action.compute.operator.exchange.RandomExchanger;
import org.elasticsearch.xpack.sql.action.compute.operator.exchange.RandomUnionSourceOperator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Fork(value = 1)
@Warmup(iterations = 1)
@Measurement(iterations = 3)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class OperatorBenchmark {

    Directory dir;
    IndexReader indexReader;

    @Param({ "100000000" }) // 100 million
    int numDocs;

    @Param({ "1", "10" })
    int maxNumSegments;

    ThreadPool threadPool;

    @Setup
    public void setup() throws IOException {
        Path path = Files.createTempDirectory("test");
        dir = new MMapDirectory(path);
        try (IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig())) {
            Document doc = new Document();
            NumericDocValuesField docValuesField = new NumericDocValuesField("value", 0);
            Random r = new Random(0);
            for (int i = 0; i < numDocs; i++) {
                doc.clear();
                docValuesField.setLongValue(r.nextLong());
                doc.add(docValuesField);
                indexWriter.addDocument(doc);
            }
            indexWriter.commit();
            indexWriter.forceMerge(maxNumSegments);
            indexWriter.flush();
        }
        indexReader = DirectoryReader.open(dir);
        threadPool = new ThreadPool(Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "OperatorBenchmark").build());
    }

    @TearDown
    public void tearDown() throws IOException {
        indexReader.close();
        dir.close();
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
    }

    private static class SimpleXORValueCollector implements Collector {

        long[] coll = new long[1];

        @Override
        public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
            SortedNumericDocValues sortedNumericDocValues = DocValues.getSortedNumeric(context.reader(), "value");
            NumericDocValues numericDocValues = DocValues.unwrapSingleton(sortedNumericDocValues);
            return new LeafCollector() {
                @Override
                public void setScorer(Scorable scorer) {
                    // ignore
                }

                @Override
                public void collect(int doc) throws IOException {
                    if (numericDocValues.advance(doc) == doc) {
                        coll[0] = numericDocValues.longValue() ^ coll[0];
                    }
                }
            };
        }

        long getVal() {
            return coll[0];
        }

        @Override
        public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE_NO_SCORES;
        }
    }

    private static class SimpleGroupCollector implements Collector {

        LongHash longHash = new LongHash(1, BigArrays.NON_RECYCLING_INSTANCE);

        @Override
        public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
            SortedNumericDocValues sortedNumericDocValues = DocValues.getSortedNumeric(context.reader(), "value");
            NumericDocValues numericDocValues = DocValues.unwrapSingleton(sortedNumericDocValues);
            return new LeafCollector() {
                @Override
                public void setScorer(Scorable scorer) {
                    // ignore
                }

                @Override
                public void collect(int doc) throws IOException {
                    if (numericDocValues.advance(doc) == doc) {
                        longHash.add(numericDocValues.longValue());
                    }
                }
            };
        }

        long getVal() {
            return longHash.size();
        }

        @Override
        public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE_NO_SCORES;
        }
    }

    private static class SimpleXOROperator implements Operator {

        private int channel;

        boolean finished;
        boolean returnedResult;

        long val;

        SimpleXOROperator(int channel) {
            this.channel = channel;
        }

        @Override
        public Page getOutput() {
            if (finished && returnedResult == false) {
                returnedResult = true;
                return new Page(new LongBlock(new long[] { val }, 1));
            }
            return null;
        }

        @Override
        public boolean isFinished() {
            return finished && returnedResult;
        }

        @Override
        public void finish() {
            finished = true;
        }

        @Override
        public boolean needsInput() {
            return true;
        }

        @Override
        public void addInput(Page page) {
            Block block = page.getBlock(channel);
            for (int i = 0; i < block.getPositionCount(); i++) {
                val = val ^ block.getLong(i);
            }
        }

        @Override
        public void close() {

        }
    }

    private static class SimpleDocsCollector implements Collector {

        long[] coll = new long[1];

        @Override
        public LeafCollector getLeafCollector(LeafReaderContext context) {
            return new LeafCollector() {
                @Override
                public void setScorer(Scorable scorer) {
                    // ignore
                }

                @Override
                public void collect(int doc) {
                    coll[0] = doc ^ coll[0];
                }
            };
        }

        long getVal() {
            return coll[0];
        }

        @Override
        public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE_NO_SCORES;
        }
    }

    @Benchmark
    public long testVisitAllDocs() throws IOException {
        IndexSearcher searcher = new IndexSearcher(indexReader);
        SimpleDocsCollector simpleDocsCollector = new SimpleDocsCollector();
        searcher.search(new MatchAllDocsQuery(), simpleDocsCollector);
        return simpleDocsCollector.getVal();
    }

    @Benchmark
    public long testVisitAllNumbers() throws IOException {
        IndexSearcher searcher = new IndexSearcher(indexReader);
        SimpleXORValueCollector simpleValueCollector = new SimpleXORValueCollector();
        searcher.search(new MatchAllDocsQuery(), simpleValueCollector);
        return simpleValueCollector.getVal();
    }

    @Benchmark
    public long testGroupAllNumbers() throws IOException {
        IndexSearcher searcher = new IndexSearcher(indexReader);
        SimpleGroupCollector simpleGroupCollector = new SimpleGroupCollector();
        searcher.search(new MatchAllDocsQuery(), simpleGroupCollector);
        return simpleGroupCollector.getVal();
    }

    private int runWithDriver(int pageSize, Operator... operators) {
        AtomicInteger rowCount = new AtomicInteger();
        List<Operator> operatorList = new ArrayList<>();
        operatorList.add(new LuceneSourceOperator(indexReader, new MatchAllDocsQuery(), pageSize));
        operatorList.addAll(List.of(operators));
        operatorList.add(new PageConsumerOperator(page -> rowCount.addAndGet(page.getPositionCount())));
        Driver driver = new Driver(operatorList, () -> {});
        driver.run();
        return rowCount.get();
    }

    @Benchmark
    public long testVisitAllNumbersBatched4K() {
        return runWithDriver(
            ByteSizeValue.ofKb(4).bytesAsInt(),
            new NumericDocValuesExtractor(indexReader, 0, 1, "value"),
            new SimpleXOROperator(2)
        );
    }

    @Benchmark
    public long testVisitAllNumbersBatched16K() {
        return runWithDriver(
            ByteSizeValue.ofKb(16).bytesAsInt(),
            new NumericDocValuesExtractor(indexReader, 0, 1, "value"),
            new SimpleXOROperator(2)
        );
    }

    @Benchmark
    public long testVisitAllDocsBatched4K() {
        return runWithDriver(ByteSizeValue.ofKb(4).bytesAsInt());
    }

    @Benchmark
    public long testVisitAllDocsBatched16K() {
        return runWithDriver(ByteSizeValue.ofKb(16).bytesAsInt());
    }

    @Benchmark
    public long testOperatorsWithLucene() {
        return runWithDriver(
            ByteSizeValue.ofKb(16).bytesAsInt(),
            new NumericDocValuesExtractor(indexReader, 0, 1, "value"),
            new LongGroupingOperator(2, BigArrays.NON_RECYCLING_INSTANCE),
            new LongMaxOperator(3), // returns largest group number
            new LongTransformerOperator(0, i -> i + 1) // adds +1 to group number (which start with 0) to get group count
        );
    }

    @Benchmark
    public long testLongAvgSingleThreadedAvg() {
        return runWithDriver(
            ByteSizeValue.ofKb(16).bytesAsInt(),
            new NumericDocValuesExtractor(indexReader, 0, 1, "value"),
            new LongAvgOperator(2), // partial reduction
            new LongAvgOperator(0, 1) // final reduction
        );
    }

    @Benchmark
    public long testLongAvgMultiThreadedAvgWithSingleThreadedSearch() {
        AtomicInteger rowCount = new AtomicInteger();
        int parallelCount = ThreadPool.searchThreadPoolSize(EsExecutors.allocatedProcessors(Settings.EMPTY));
        List<Driver> drivers = new ArrayList<>(parallelCount);
        List<ExchangeSource> forkExchangeSources = new ArrayList<>(parallelCount);
        List<ExchangeSource> joinExchangeSources = new ArrayList<>(parallelCount);
        for (int i = 0; i < parallelCount; i++) {
            ExchangeSource forkExchangeSource = new ExchangeSource();
            forkExchangeSources.add(forkExchangeSource);
            ExchangeSource joinExchangeSource = new ExchangeSource();
            joinExchangeSources.add(joinExchangeSource);
            List<Operator> operatorList = new ArrayList<>();
            operatorList.add(new ExchangeSourceOperator(forkExchangeSource));
            operatorList.addAll(
                List.of(
                    new NumericDocValuesExtractor(indexReader, 0, 1, "value"),
                    new LongAvgOperator(2), // PARTIAL
                    new ExchangeSinkOperator(
                        new ExchangeSink(new PassthroughExchanger(joinExchangeSource, Integer.MAX_VALUE), s -> joinExchangeSource.finish())
                    )
                )
            );
            Driver driver = new Driver(operatorList, () -> {});
            drivers.add(driver);
        }

        Driver luceneDriver = new Driver(
            List.of(
                new LuceneSourceOperator(indexReader, new MatchAllDocsQuery(), ByteSizeValue.ofKb(16).bytesAsInt()),
                new ExchangeSinkOperator(
                    new ExchangeSink(
                        new RandomExchanger(
                            forkExchangeSources.stream()
                                .map(exchangeSource -> (Consumer<Page>) page -> exchangeSource.addPage(page, () -> {}))
                                .collect(Collectors.toList())
                        ),
                        sink -> forkExchangeSources.stream().forEach(ExchangeSource::finish)
                    )
                )
            ),
            () -> {}
        );
        drivers.add(luceneDriver);

        Driver reduceDriver = new Driver(
            List.of(
                new RandomUnionSourceOperator(joinExchangeSources),
                new LongAvgOperator(0, 1), // FINAL
                new PageConsumerOperator(page -> rowCount.addAndGet(page.getPositionCount()))
            ),
            () -> {}
        );
        drivers.add(reduceDriver);

        Driver.runToCompletion(threadPool.executor(ThreadPool.Names.SEARCH), drivers);
        return rowCount.get();
    }

    @Benchmark
    public long testLongAvgMultiThreadedAvgWithMultiThreadedSearch() {
        AtomicInteger rowCount = new AtomicInteger();
        int parallelCount = ThreadPool.searchThreadPoolSize(EsExecutors.allocatedProcessors(Settings.EMPTY));
        List<Driver> drivers = new ArrayList<>(parallelCount);
        List<ExchangeSource> joinExchangeSources = new ArrayList<>(parallelCount);

        for (LuceneSourceOperator luceneSourceOperator : new LuceneSourceOperator(
            indexReader,
            new MatchAllDocsQuery(),
            ByteSizeValue.ofKb(16).bytesAsInt()
        ).slice(parallelCount)) {
            ExchangeSource joinExchangeSource = new ExchangeSource();
            joinExchangeSources.add(joinExchangeSource);
            Driver driver = new Driver(
                List.of(
                    luceneSourceOperator,
                    new NumericDocValuesExtractor(indexReader, 0, 1, "value"),
                    new LongAvgOperator(2), // PARTIAL
                    new ExchangeSinkOperator(
                        new ExchangeSink(new PassthroughExchanger(joinExchangeSource, Integer.MAX_VALUE), s -> joinExchangeSource.finish())
                    )
                ),
                () -> {}
            );
            drivers.add(driver);
        }

        Driver reduceDriver = new Driver(
            List.of(
                new RandomUnionSourceOperator(joinExchangeSources),
                new LongAvgOperator(0, 1), // FINAL
                new PageConsumerOperator(page -> rowCount.addAndGet(page.getPositionCount()))
            ),
            () -> {}
        );
        drivers.add(reduceDriver);

        Driver.runToCompletion(threadPool.executor(ThreadPool.Names.SEARCH), drivers);
        return rowCount.get();
    }

    @Benchmark
    public long testLongAvgMultiThreadedAvgWithMultiThreadedSegmentSearch() {
        AtomicInteger rowCount = new AtomicInteger();
        List<Driver> drivers = new ArrayList<>();
        List<ExchangeSource> joinExchangeSources = new ArrayList<>();

        for (LuceneSourceOperator luceneSourceOperator : new LuceneSourceOperator(
            indexReader,
            new MatchAllDocsQuery(),
            ByteSizeValue.ofKb(16).bytesAsInt()
        ).segmentSlice()) {
            ExchangeSource joinExchangeSource = new ExchangeSource();
            joinExchangeSources.add(joinExchangeSource);
            Driver driver = new Driver(
                List.of(
                    luceneSourceOperator,
                    new NumericDocValuesExtractor(indexReader, 0, 1, "value"),
                    new LongAvgOperator(2), // PARTIAL
                    new ExchangeSinkOperator(
                        new ExchangeSink(new PassthroughExchanger(joinExchangeSource, Integer.MAX_VALUE), s -> joinExchangeSource.finish())
                    )
                ),
                () -> {}
            );
            drivers.add(driver);
        }

        Driver reduceDriver = new Driver(
            List.of(
                new RandomUnionSourceOperator(joinExchangeSources),
                new LongAvgOperator(0, 1), // FINAL
                new PageConsumerOperator(page -> rowCount.addAndGet(page.getPositionCount()))
            ),
            () -> {}
        );
        drivers.add(reduceDriver);

        Driver.runToCompletion(threadPool.executor(ThreadPool.Names.SEARCH), drivers);
        return rowCount.get();
    }
}
