/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.benchmark.compute.operator;

import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.LuceneSourceOperator;
import org.elasticsearch.compute.lucene.ValueSourceInfo;
import org.elasticsearch.compute.lucene.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.TopNOperator;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.plain.SortedDoublesIndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedNumericIndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.script.field.KeywordDocValuesField;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.FieldContext;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@Warmup(iterations = 5)
@Measurement(iterations = 7)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(1)
public class ValuesSourceReaderBenchmark {
    private static final int BLOCK_LENGTH = 16 * 1024;
    private static final int INDEX_SIZE = 10 * BLOCK_LENGTH;
    private static final int COMMIT_INTERVAL = 500;

    static {
        // Smoke test all the expected values and force loading subclasses more like prod
        try {
            ValuesSourceReaderBenchmark benchmark = new ValuesSourceReaderBenchmark();
            benchmark.setupIndex();
            try {
                for (String layout : ValuesSourceReaderBenchmark.class.getField("layout").getAnnotationsByType(Param.class)[0].value()) {
                    for (String name : ValuesSourceReaderBenchmark.class.getField("name").getAnnotationsByType(Param.class)[0].value()) {
                        benchmark.layout = layout;
                        benchmark.name = name;
                        benchmark.setupPages();
                        benchmark.benchmark();
                    }
                }
            } finally {
                benchmark.teardownIndex();
            }
        } catch (IOException | NoSuchFieldException e) {
            throw new AssertionError(e);
        }
    }

    private static ValueSourceInfo info(IndexReader reader, String name) {
        return switch (name) {
            case "long" -> numericInfo(reader, name, IndexNumericFieldData.NumericType.LONG, ElementType.LONG);
            case "int" -> numericInfo(reader, name, IndexNumericFieldData.NumericType.INT, ElementType.INT);
            case "double" -> {
                SortedDoublesIndexFieldData fd = new SortedDoublesIndexFieldData(
                    name,
                    IndexNumericFieldData.NumericType.DOUBLE,
                    CoreValuesSourceType.NUMERIC,
                    null
                );
                FieldContext context = new FieldContext(name, fd, null);
                yield new ValueSourceInfo(
                    CoreValuesSourceType.NUMERIC,
                    CoreValuesSourceType.NUMERIC.getField(context, null),
                    ElementType.DOUBLE,
                    reader
                );
            }
            case "keyword" -> {
                SortedSetOrdinalsIndexFieldData fd = new SortedSetOrdinalsIndexFieldData(
                    new IndexFieldDataCache.None(),
                    "keyword",
                    CoreValuesSourceType.KEYWORD,
                    new NoneCircuitBreakerService(),
                    (dv, n) -> new KeywordDocValuesField(FieldData.toString(dv), n)
                );
                FieldContext context = new FieldContext(name, fd, null);
                yield new ValueSourceInfo(
                    CoreValuesSourceType.KEYWORD,
                    CoreValuesSourceType.KEYWORD.getField(context, null),
                    ElementType.BYTES_REF,
                    reader
                );
            }
            default -> throw new IllegalArgumentException("can't read [" + name + "]");
        };
    }

    private static ValueSourceInfo numericInfo(
        IndexReader reader,
        String name,
        IndexNumericFieldData.NumericType numericType,
        ElementType elementType
    ) {
        SortedNumericIndexFieldData fd = new SortedNumericIndexFieldData(name, numericType, CoreValuesSourceType.NUMERIC, null);
        FieldContext context = new FieldContext(name, fd, null);
        return new ValueSourceInfo(CoreValuesSourceType.NUMERIC, CoreValuesSourceType.NUMERIC.getField(context, null), elementType, reader);
    }

    /**
     * Layouts for the input blocks.
     * <ul>
     * <li>{@code in_order} is how {@link LuceneSourceOperator} produces them to read in
     *     the most efficient possible way. We </li>
     * <li>{@code shuffled} is chunked the same size as {@link LuceneSourceOperator} but
     *     loads in a shuffled order, like a hypothetical {@link TopNOperator} that can
     *     output large blocks would output.</li>
     * <li>{@code shuffled_singles} is shuffled in the same order as {@code shuffled} but
     *     each page has a single document rather than {@code BLOCK_SIZE} docs.</li>
     * </ul>
     */
    @Param({ "in_order", "shuffled", "shuffled_singles" })
    public String layout;

    @Param({ "long", "int", "double", "keyword" })
    public String name;

    private Directory directory;
    private IndexReader reader;
    private List<Page> pages;

    @Benchmark
    @OperationsPerInvocation(INDEX_SIZE)
    public void benchmark() {
        ValuesSourceReaderOperator op = new ValuesSourceReaderOperator(List.of(info(reader, name)), 0, name);
        long sum = 0;
        for (Page page : pages) {
            op.addInput(page);
            switch (name) {
                case "long" -> {
                    LongVector values = op.getOutput().<LongBlock>getBlock(1).asVector();
                    for (int p = 0; p < values.getPositionCount(); p++) {
                        sum += values.getLong(p);
                    }
                }
                case "int" -> {
                    IntVector values = op.getOutput().<IntBlock>getBlock(1).asVector();
                    for (int p = 0; p < values.getPositionCount(); p++) {
                        sum += values.getInt(p);
                    }
                }
                case "double" -> {
                    DoubleVector values = op.getOutput().<DoubleBlock>getBlock(1).asVector();
                    for (int p = 0; p < values.getPositionCount(); p++) {
                        sum += values.getDouble(p);
                    }
                }
                case "keyword" -> {
                    BytesRef scratch = new BytesRef();
                    BytesRefVector values = op.getOutput().<BytesRefBlock>getBlock(1).asVector();
                    for (int p = 0; p < values.getPositionCount(); p++) {
                        sum += Integer.parseInt(values.getBytesRef(p, scratch).utf8ToString());
                    }
                }
            }
        }
        long expected = INDEX_SIZE;
        expected = expected * (expected - 1) / 2;
        if (expected != sum) {
            throw new AssertionError("[" + layout + "][" + name + "] expected [" + expected + "] but was [" + sum + "]");
        }
    }

    @Setup
    public void setup() throws IOException {
        setupIndex();
        setupPages();
    }

    private void setupIndex() throws IOException {
        directory = new ByteBuffersDirectory();
        try (IndexWriter iw = new IndexWriter(directory, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
            for (int i = 0; i < INDEX_SIZE; i++) {
                iw.addDocument(
                    List.of(
                        new NumericDocValuesField("long", i),
                        new NumericDocValuesField("int", i),
                        new NumericDocValuesField("double", NumericUtils.doubleToSortableLong(i)),
                        new KeywordFieldMapper.KeywordField(
                            "keyword",
                            new BytesRef(Integer.toString(i)),
                            KeywordFieldMapper.Defaults.FIELD_TYPE
                        )
                    )
                );
                if (i % COMMIT_INTERVAL == 0) {
                    iw.commit();
                }
            }
        }
        reader = DirectoryReader.open(directory);
    }

    private void setupPages() {
        pages = new ArrayList<>();
        switch (layout) {
            case "in_order" -> {
                IntVector.Builder docs = IntVector.newVectorBuilder(BLOCK_LENGTH);
                for (LeafReaderContext ctx : reader.leaves()) {
                    int begin = 0;
                    while (begin < ctx.reader().maxDoc()) {
                        int end = Math.min(begin + BLOCK_LENGTH, ctx.reader().maxDoc());
                        for (int doc = 0; doc < ctx.reader().maxDoc(); doc++) {
                            docs.appendInt(doc);
                        }
                        pages.add(
                            new Page(
                                new DocVector(
                                    IntBlock.newConstantBlockWith(0, end - begin).asVector(),
                                    IntBlock.newConstantBlockWith(ctx.ord, end - begin).asVector(),
                                    docs.build(),
                                    true
                                ).asBlock()
                            )
                        );
                        docs = IntVector.newVectorBuilder(BLOCK_LENGTH);
                        begin = end;
                    }
                }
            }
            case "shuffled" -> {
                record ItrAndOrd(PrimitiveIterator.OfInt itr, int ord) {}
                List<ItrAndOrd> docItrs = new ArrayList<>(reader.leaves().size());
                for (LeafReaderContext ctx : reader.leaves()) {
                    docItrs.add(new ItrAndOrd(IntStream.range(0, ctx.reader().maxDoc()).iterator(), ctx.ord));
                }
                IntVector.Builder docs = IntVector.newVectorBuilder(BLOCK_LENGTH);
                IntVector.Builder leafs = IntVector.newVectorBuilder(BLOCK_LENGTH);
                int size = 0;
                while (docItrs.isEmpty() == false) {
                    Iterator<ItrAndOrd> itrItr = docItrs.iterator();
                    while (itrItr.hasNext()) {
                        ItrAndOrd next = itrItr.next();
                        if (false == next.itr.hasNext()) {
                            itrItr.remove();
                            continue;
                        }
                        docs.appendInt(next.itr.nextInt());
                        leafs.appendInt(next.ord);
                        size++;
                        if (size >= BLOCK_LENGTH) {
                            pages.add(
                                new Page(
                                    new DocVector(IntBlock.newConstantBlockWith(0, size).asVector(), leafs.build(), docs.build(), null)
                                        .asBlock()
                                )
                            );
                            docs = IntVector.newVectorBuilder(BLOCK_LENGTH);
                            leafs = IntVector.newVectorBuilder(BLOCK_LENGTH);
                            size = 0;
                        }
                    }
                }
                if (size > 0) {
                    pages.add(
                        new Page(
                            new DocVector(
                                IntBlock.newConstantBlockWith(0, size).asVector(),
                                leafs.build().asBlock().asVector(),
                                docs.build(),
                                null
                            ).asBlock()
                        )
                    );
                }
            }
            case "shuffled_singles" -> {
                record ItrAndOrd(PrimitiveIterator.OfInt itr, int ord) {}
                List<ItrAndOrd> docItrs = new ArrayList<>(reader.leaves().size());
                for (LeafReaderContext ctx : reader.leaves()) {
                    docItrs.add(new ItrAndOrd(IntStream.range(0, ctx.reader().maxDoc()).iterator(), ctx.ord));
                }
                while (docItrs.isEmpty() == false) {
                    Iterator<ItrAndOrd> itrItr = docItrs.iterator();
                    while (itrItr.hasNext()) {
                        ItrAndOrd next = itrItr.next();
                        if (false == next.itr.hasNext()) {
                            itrItr.remove();
                            continue;
                        }
                        pages.add(
                            new Page(
                                new DocVector(
                                    IntBlock.newConstantBlockWith(0, 1).asVector(),
                                    IntBlock.newConstantBlockWith(next.ord, 1).asVector(),
                                    IntBlock.newConstantBlockWith(next.itr.nextInt(), 1).asVector(),
                                    true
                                ).asBlock()
                            )
                        );
                    }
                }
            }
            default -> throw new IllegalArgumentException("unsupported layout [" + layout + "]");
        }
    }

    @TearDown
    public void teardownIndex() throws IOException {
        IOUtils.close(reader, directory);
    }
}
