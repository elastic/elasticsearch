/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.compute.operator;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
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
import org.elasticsearch.compute.lucene.ShardRefCounted;
import org.elasticsearch.compute.lucene.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.topn.TopNOperator;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
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
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.Set;
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
    private static final BigArrays BIG_ARRAYS = BigArrays.NON_RECYCLING_INSTANCE;
    private static final BlockFactory blockFactory = BlockFactory.getInstance(
        new NoopCircuitBreaker("noop"),
        BigArrays.NON_RECYCLING_INSTANCE
    );

    static {
        // Smoke test all the expected values and force loading subclasses more like prod
        selfTest();
    }

    static void selfTest() {
        try {
            ValuesSourceReaderBenchmark benchmark = new ValuesSourceReaderBenchmark();
            benchmark.setupIndex();
            try {
                for (String layout : ValuesSourceReaderBenchmark.class.getField("layout").getAnnotationsByType(Param.class)[0].value()) {
                    for (String name : ValuesSourceReaderBenchmark.class.getField("name").getAnnotationsByType(Param.class)[0].value()) {
                        benchmark.layout = layout;
                        benchmark.name = name;
                        try {
                            benchmark.setupPages();
                            benchmark.benchmark();
                        } catch (Exception e) {
                            throw new AssertionError("error initializing [" + layout + "/" + name + "]", e);
                        }
                    }
                }
            } finally {
                benchmark.teardownIndex();
            }
        } catch (IOException | NoSuchFieldException e) {
            throw new AssertionError(e);
        }
    }

    private static List<ValuesSourceReaderOperator.FieldInfo> fields(String name) {
        return switch (name) {
            case "3_stored_keywords" -> List.of(
                new ValuesSourceReaderOperator.FieldInfo("keyword_1", ElementType.BYTES_REF, shardIdx -> blockLoader("stored_keyword_1")),
                new ValuesSourceReaderOperator.FieldInfo("keyword_2", ElementType.BYTES_REF, shardIdx -> blockLoader("stored_keyword_2")),
                new ValuesSourceReaderOperator.FieldInfo("keyword_3", ElementType.BYTES_REF, shardIdx -> blockLoader("stored_keyword_3"))
            );
            default -> List.of(new ValuesSourceReaderOperator.FieldInfo(name, elementType(name), shardIdx -> blockLoader(name)));
        };
    }

    enum Where {
        DOC_VALUES,
        SOURCE,
        STORED;
    }

    private static ElementType elementType(String name) {
        name = WhereAndBaseName.fromName(name).name;
        switch (name) {
            case "long":
                return ElementType.LONG;
            case "int":
                return ElementType.INT;
            case "double":
                return ElementType.DOUBLE;
        }
        if (name.startsWith("keyword")) {
            return ElementType.BYTES_REF;
        }
        throw new UnsupportedOperationException("no element type for [" + name + "]");
    }

    private static BlockLoader blockLoader(String name) {
        WhereAndBaseName w = WhereAndBaseName.fromName(name);
        switch (w.name) {
            case "long":
                return numericBlockLoader(w, NumberFieldMapper.NumberType.LONG);
            case "int":
                return numericBlockLoader(w, NumberFieldMapper.NumberType.INTEGER);
            case "double":
                return numericBlockLoader(w, NumberFieldMapper.NumberType.DOUBLE);
            case "keyword":
                w = new WhereAndBaseName(w.where, "keyword_1");
        }
        if (w.name.startsWith("keyword")) {
            boolean syntheticSource = false;
            FieldType ft = new FieldType(KeywordFieldMapper.Defaults.FIELD_TYPE);
            switch (w.where) {
                case DOC_VALUES:
                    break;
                case SOURCE:
                    ft.setDocValuesType(DocValuesType.NONE);
                    break;
                case STORED:
                    ft.setStored(true);
                    ft.setDocValuesType(DocValuesType.NONE);
                    syntheticSource = true;
                    break;
            }
            ft.freeze();
            return new KeywordFieldMapper.KeywordFieldType(
                w.name,
                ft,
                Lucene.KEYWORD_ANALYZER,
                Lucene.KEYWORD_ANALYZER,
                Lucene.KEYWORD_ANALYZER,
                new KeywordFieldMapper.Builder(name, IndexVersion.current()).docValues(ft.docValuesType() != DocValuesType.NONE),
                syntheticSource
            ).blockLoader(new MappedFieldType.BlockLoaderContext() {
                @Override
                public String indexName() {
                    return "benchmark";
                }

                @Override
                public IndexSettings indexSettings() {
                    throw new UnsupportedOperationException();
                }

                @Override
                public MappedFieldType.FieldExtractPreference fieldExtractPreference() {
                    return MappedFieldType.FieldExtractPreference.NONE;
                }

                @Override
                public SearchLookup lookup() {
                    throw new UnsupportedOperationException();
                }

                @Override
                public Set<String> sourcePaths(String name) {
                    return Set.of(name);
                }

                @Override
                public String parentField(String field) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public FieldNamesFieldMapper.FieldNamesFieldType fieldNames() {
                    return FieldNamesFieldMapper.FieldNamesFieldType.get(true);
                }
            });
        }
        throw new IllegalArgumentException("can't read [" + name + "]");
    }

    private record WhereAndBaseName(Where where, String name) {
        static WhereAndBaseName fromName(String name) {
            if (name.startsWith("stored_")) {
                return new WhereAndBaseName(Where.STORED, name.substring("stored_".length()));
            } else if (name.startsWith("source_")) {
                return new WhereAndBaseName(Where.SOURCE, name.substring("source_".length()));
            }
            return new WhereAndBaseName(Where.DOC_VALUES, name);
        }
    }

    private static BlockLoader numericBlockLoader(WhereAndBaseName w, NumberFieldMapper.NumberType numberType) {
        boolean stored = false;
        boolean docValues = true;
        switch (w.where) {
            case DOC_VALUES:
                break;
            case SOURCE:
                stored = true;
                docValues = false;
                break;
            case STORED:
                throw new UnsupportedOperationException();
        }
        return new NumberFieldMapper.NumberFieldType(
            w.name,
            numberType,
            true,
            stored,
            docValues,
            true,
            null,
            Map.of(),
            null,
            false,
            null,
            null,
            false
        ).blockLoader(new MappedFieldType.BlockLoaderContext() {
            @Override
            public String indexName() {
                return "benchmark";
            }

            @Override
            public MappedFieldType.FieldExtractPreference fieldExtractPreference() {
                return MappedFieldType.FieldExtractPreference.NONE;
            }

            @Override
            public IndexSettings indexSettings() {
                throw new UnsupportedOperationException();
            }

            @Override
            public SearchLookup lookup() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Set<String> sourcePaths(String name) {
                throw new UnsupportedOperationException();
            }

            @Override
            public String parentField(String field) {
                throw new UnsupportedOperationException();
            }

            @Override
            public FieldNamesFieldMapper.FieldNamesFieldType fieldNames() {
                throw new UnsupportedOperationException();
            }
        });
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

    @Param({ "long", "int", "double", "keyword", "stored_keyword", "3_stored_keywords" })
    public String name;

    private Directory directory;
    private IndexReader reader;
    private List<Page> pages;

    @Benchmark
    @OperationsPerInvocation(INDEX_SIZE)
    public void benchmark() {
        ValuesSourceReaderOperator op = new ValuesSourceReaderOperator(
            blockFactory,
            fields(name),
            List.of(new ValuesSourceReaderOperator.ShardContext(reader, () -> {
                throw new UnsupportedOperationException("can't load _source here");
            }, EsqlPlugin.STORED_FIELDS_SEQUENTIAL_PROPORTION.getDefault(Settings.EMPTY))),
            0
        );
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
                        sum += (long) values.getDouble(p);
                    }
                }
                case "keyword", "stored_keyword" -> {
                    BytesRef scratch = new BytesRef();
                    BytesRefVector values = op.getOutput().<BytesRefBlock>getBlock(1).asVector();
                    for (int p = 0; p < values.getPositionCount(); p++) {
                        BytesRef r = values.getBytesRef(p, scratch);
                        r.offset++;
                        r.length--;
                        sum += Integer.parseInt(r.utf8ToString());
                    }
                }
                case "3_stored_keywords" -> {
                    BytesRef scratch = new BytesRef();
                    Page out = op.getOutput();
                    for (BytesRefVector values : new BytesRefVector[] {
                        out.<BytesRefBlock>getBlock(1).asVector(),
                        out.<BytesRefBlock>getBlock(2).asVector(),
                        out.<BytesRefBlock>getBlock(3).asVector() }) {

                        for (int p = 0; p < values.getPositionCount(); p++) {
                            BytesRef r = values.getBytesRef(p, scratch);
                            r.offset++;
                            r.length--;
                            sum += Integer.parseInt(r.utf8ToString());
                        }
                    }
                }
            }
        }
        long expected = 0;
        switch (name) {
            case "keyword", "stored_keyword":
                for (int i = 0; i < INDEX_SIZE; i++) {
                    expected += i % 1000;
                }
                break;
            case "3_stored_keywords":
                for (int i = 0; i < INDEX_SIZE; i++) {
                    expected += 3 * (i % 1000);
                }
                break;
            default:
                expected = INDEX_SIZE;
                expected = expected * (expected - 1) / 2;
        }
        if (expected != sum) {
            throw new AssertionError("[" + layout + "][" + name + "] expected [" + expected + "] but was [" + sum + "]");
        }
        boolean foundStoredFieldLoader = false;
        ValuesSourceReaderOperator.Status status = (ValuesSourceReaderOperator.Status) op.status();
        for (Map.Entry<String, Integer> e : status.readersBuilt().entrySet()) {
            if (e.getKey().indexOf("stored_fields") >= 0) {
                foundStoredFieldLoader = true;
            }
        }
        if (name.indexOf("stored") >= 0) {
            if (foundStoredFieldLoader == false) {
                throw new AssertionError("expected to use a stored field loader but only had: " + status.readersBuilt());
            }
        } else {
            if (foundStoredFieldLoader) {
                throw new AssertionError("expected not to use a stored field loader but only had: " + status.readersBuilt());
            }
        }
    }

    @Setup
    public void setup() throws IOException {
        setupIndex();
        setupPages();
    }

    private void setupIndex() throws IOException {
        directory = new ByteBuffersDirectory();
        FieldType keywordFieldType = new FieldType(KeywordFieldMapper.Defaults.FIELD_TYPE);
        keywordFieldType.setStored(true);
        keywordFieldType.freeze();
        try (IndexWriter iw = new IndexWriter(directory, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
            for (int i = 0; i < INDEX_SIZE; i++) {
                String c = Character.toString('a' - ((i % 1000) % 26) + 26);
                iw.addDocument(
                    List.of(
                        new NumericDocValuesField("long", i),
                        new StoredField("long", i),
                        new NumericDocValuesField("int", i),
                        new StoredField("int", i),
                        new NumericDocValuesField("double", NumericUtils.doubleToSortableLong(i)),
                        new StoredField("double", (double) i),
                        new KeywordFieldMapper.KeywordField("keyword_1", new BytesRef(c + i % 1000), keywordFieldType),
                        new KeywordFieldMapper.KeywordField("keyword_2", new BytesRef(c + i % 1000), keywordFieldType),
                        new KeywordFieldMapper.KeywordField("keyword_3", new BytesRef(c + i % 1000), keywordFieldType)
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
                IntVector.Builder docs = blockFactory.newIntVectorBuilder(BLOCK_LENGTH);
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
                                    ShardRefCounted.ALWAYS_REFERENCED,
                                    blockFactory.newConstantIntBlockWith(0, end - begin).asVector(),
                                    blockFactory.newConstantIntBlockWith(ctx.ord, end - begin).asVector(),
                                    docs.build(),
                                    true
                                ).asBlock()
                            )
                        );
                        docs = blockFactory.newIntVectorBuilder(BLOCK_LENGTH);
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
                IntVector.Builder docs = blockFactory.newIntVectorBuilder(BLOCK_LENGTH);
                IntVector.Builder leafs = blockFactory.newIntVectorBuilder(BLOCK_LENGTH);
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
                                    new DocVector(

                                        ShardRefCounted.ALWAYS_REFERENCED,
                                        blockFactory.newConstantIntVector(0, size),
                                        leafs.build(),
                                        docs.build(),
                                        null
                                    ).asBlock()
                                )
                            );
                            docs = blockFactory.newIntVectorBuilder(BLOCK_LENGTH);
                            leafs = blockFactory.newIntVectorBuilder(BLOCK_LENGTH);
                            size = 0;
                        }
                    }
                }
                if (size > 0) {
                    pages.add(
                        new Page(
                            new DocVector(

                                ShardRefCounted.ALWAYS_REFERENCED,
                                blockFactory.newConstantIntBlockWith(0, size).asVector(),
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

                                    ShardRefCounted.ALWAYS_REFERENCED,
                                    blockFactory.newConstantIntVector(0, 1),
                                    blockFactory.newConstantIntVector(next.ord, 1),
                                    blockFactory.newConstantIntVector(next.itr.nextInt(), 1),
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
