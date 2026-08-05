/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark._nightly.esql;

import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.AlwaysReferencedIndexedByShardId;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromSingleton;
import org.elasticsearch.compute.lucene.query.LuceneSourceOperator;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.topn.TopNOperator;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.index.mapper.blockloader.Warnings;
import org.elasticsearch.index.mapper.flattened.ExtractFlattenedSubfieldConfig;
import org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.FieldExtract;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
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
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

/**
 * Microbenchmark for the three ways ES|QL reads a sub-field out of a {@code flattened} field, so the
 * relative cost of each can be quantified and any GA performance work on {@code field_extract} can be
 * measured against a stable baseline.
 * <ul>
 *   <li>{@code keyed_fused} - the fast path: {@code field_extract(root, "constant")} fuses into the
 *       {@link org.elasticsearch.index.mapper.flattened.KeyedFlattenedDocValuesBlockLoader}, reading a
 *       single sub-key's doc values directly. Selected by handing the mapper an
 *       {@link ExtractFlattenedSubfieldConfig} via the block-loader context.</li>
 *   <li>{@code root_then_evaluator} - the slow fallback: the whole flattened root is loaded as a JSON
 *       blob (as {@code root_only} does) and then {@link FieldExtract#process} re-parses that blob per
 *       row to pull out the key. This is {@code root_only}'s cost plus the per-row parse.</li>
 *   <li>{@code root_only} - the baseline: load the entire flattened object via
 *       {@link org.elasticsearch.index.mapper.flattened.RootFlattenedDocValuesBlockLoader} with no
 *       extraction, so {@code (root_then_evaluator - root_only)} isolates the per-row parse cost.</li>
 * </ul>
 * Both loaders read the same {@code field._keyed} doc values, so a single index build feeds all paths;
 * only the constructed {@link org.elasticsearch.index.mapper.BlockLoader} differs. The {@code subFields}
 * parameter controls how many sibling keys each document carries, which grows the root blob (and thus the
 * parse cost) while leaving the fused single-key read unchanged.
 */
@Warmup(iterations = 5)
@Measurement(iterations = 7)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(1)
public class FlattenedFieldExtractBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    private static final String FIELD = "field";
    private static final String KEYED_FIELD = FIELD + "._keyed";
    private static final String SEPARATOR = "\0";
    /** The sub-key extracted by the {@code keyed_fused} and {@code root_then_evaluator} paths. */
    private static final String KEY = "sub_0";
    private static final BytesRef KEY_BYTES = new BytesRef(KEY);

    private static final String[] SUPPORTED_LAYOUTS = new String[] { "in_order", "shuffled" };
    private static final String[] SUPPORTED_PATHS = new String[] { "keyed_fused", "root_then_evaluator", "root_only" };
    private static final int[] SUPPORTED_SUB_FIELDS = new int[] { 5, 20, 100 };

    private static final int BLOCK_LENGTH = 16 * 1024;
    private static final int INDEX_SIZE = 5 * BLOCK_LENGTH;
    /**
     * A tiny index size used only by {@link #selfTest()}. The self-test runs the whole
     * {@code layout x path x subFields} correctness matrix inside every JMH fork, so using the full
     * {@link #INDEX_SIZE} there makes the {@code root_*} paths (which reconstruct the entire flattened
     * object per document) dominate fork startup. A few thousand docs still exercises multiple segments
     * and the {@code i % VALUE_MOD} checksum wraparound while keeping each fork's self-test near-instant.
     */
    private static final int SELF_TEST_INDEX_SIZE = 2048;
    private static final int COMMIT_INTERVAL = 500;
    /** The extracted value for a document is {@code i % VALUE_MOD}, so the checksum is order-independent. */
    private static final int VALUE_MOD = 1000;
    private static final BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("none"))
        .build();

    public static IndexSettings defaultIndexSettings() {
        IndexMetadata indexMetadata = IndexMetadata.builder("index")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        return new IndexSettings(indexMetadata, Settings.EMPTY);
    }

    static {
        // Smoke test all the expected values and force loading subclasses more like prod.
        if (false == "true".equals(System.getProperty("skipSelfTest"))) {
            selfTest();
        }
    }

    static void selfTest() {
        try {
            FlattenedFieldExtractBenchmark benchmark = new FlattenedFieldExtractBenchmark();
            benchmark.indexSize = SELF_TEST_INDEX_SIZE;
            for (int subFields : SUPPORTED_SUB_FIELDS) {
                benchmark.subFields = subFields;
                benchmark.setupIndex();
                try {
                    for (String layout : SUPPORTED_LAYOUTS) {
                        benchmark.layout = layout;
                        benchmark.setupPages();
                        for (String path : SUPPORTED_PATHS) {
                            benchmark.path = path;
                            try {
                                benchmark.benchmark();
                            } catch (Exception e) {
                                throw new AssertionError("error initializing [" + layout + "/" + path + "/" + subFields + "]", e);
                            }
                        }
                    }
                } finally {
                    benchmark.teardownIndex();
                }
            }
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Layouts for the input blocks, matching {@link ValuesSourceReaderBenchmark}: {@code in_order} is how
     * {@link LuceneSourceOperator} emits docs for the most efficient read, while {@code shuffled} models a
     * large-block {@link TopNOperator}-style out-of-order read.
     */
    @Param({ "in_order", "shuffled" })
    public String layout;

    @Param({ "keyed_fused", "root_then_evaluator", "root_only" })
    public String path;

    @Param({ "5", "20", "100" })
    public int subFields;

    private Directory directory;
    private IndexReader reader;
    private List<Page> pages;
    private FlattenedFieldMapper.RootFlattenedFieldType fieldType;
    /** Number of indexed docs. {@link #INDEX_SIZE} for measured runs; {@link #SELF_TEST_INDEX_SIZE} for the self-test. */
    private int indexSize = INDEX_SIZE;

    @Benchmark
    @OperationsPerInvocation(INDEX_SIZE)
    public void benchmark() {
        BlockLoaderFunctionConfig config = path.equals("keyed_fused") ? new ExtractFlattenedSubfieldConfig(KEY) : null;
        ElementType elementType = ElementType.BYTES_REF;
        List<ValuesSourceReaderOperator.FieldInfo> fields = List.of(
            new ValuesSourceReaderOperator.FieldInfo(
                FIELD,
                elementType,
                false,
                (ctx, shardIdx) -> ValuesSourceReaderOperator.load(fieldType.blockLoader(new BenchContext(config)))
            )
        );
        boolean reuseColumnLoaders = fields.size() <= PlannerSettings.REUSE_COLUMN_LOADERS_THRESHOLD.get(Settings.EMPTY);
        ValuesSourceReaderOperator op = new ValuesSourceReaderOperator(
            new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null),
            ByteSizeValue.ofMb(1).getBytes(),
            fields,
            new IndexedByShardIdFromSingleton<>(new ValuesSourceReaderOperator.ShardContext(reader, (sourcePaths) -> {
                throw new UnsupportedOperationException("can't load _source here");
            }, EsqlPlugin.STORED_FIELDS_SEQUENTIAL_PROPORTION.getDefault(Settings.EMPTY))),
            reuseColumnLoaders,
            0,
            PlannerSettings.SOURCE_RESERVATION_FACTOR.getDefault(Settings.EMPTY),
            PlannerSettings.DOC_SEQUENCE_BYTES_REF_FIELD_THRESHOLD.getDefault(Settings.EMPTY),
            () -> 0L
        );
        long sum = 0;
        long positions = 0;
        BytesRef scratch = new BytesRef();
        for (Page page : pages) {
            op.addInput(page.shallowCopy());
            BytesRefBlock loaded = op.getOutput().<BytesRefBlock>getBlock(1);
            switch (path) {
                case "keyed_fused" -> sum += sumSingleValued(loaded, scratch);
                case "root_only" -> positions += loaded.getPositionCount();
                case "root_then_evaluator" -> sum += extractAndSum(loaded, scratch);
            }
        }
        switch (path) {
            case "keyed_fused", "root_then_evaluator" -> {
                long expected = 0;
                for (int i = 0; i < indexSize; i++) {
                    expected += i % VALUE_MOD;
                }
                if (expected != sum) {
                    throw new AssertionError(
                        "[" + layout + "][" + path + "][" + subFields + "] expected [" + expected + "] but was [" + sum + "]"
                    );
                }
            }
            case "root_only" -> {
                if (positions != indexSize) {
                    throw new AssertionError(
                        "["
                            + layout
                            + "]["
                            + path
                            + "]["
                            + subFields
                            + "] expected ["
                            + indexSize
                            + "] positions but was ["
                            + positions
                            + "]"
                    );
                }
            }
        }
    }

    /**
     * Sums the integer value at every position of a single-valued keyword block. Used by the fused path,
     * whose loader emits the extracted sub-key directly.
     */
    private static long sumSingleValued(BytesRefBlock block, BytesRef scratch) {
        long sum = 0;
        for (int p = 0; p < block.getPositionCount(); p++) {
            if (block.isNull(p)) {
                throw new AssertionError("unexpected null at position [" + p + "]");
            }
            sum += Integer.parseInt(block.getBytesRef(block.getFirstValueIndex(p), scratch).utf8ToString());
        }
        return sum;
    }

    /**
     * Runs the per-row fallback evaluator ({@link FieldExtract#process}) over a block of whole flattened
     * JSON blobs, mirroring what the slow path does after the root loader materializes each document.
     */
    private long extractAndSum(BytesRefBlock rootBlobs, BytesRef scratch) {
        long sum = 0;
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(rootBlobs.getPositionCount())) {
            for (int p = 0; p < rootBlobs.getPositionCount(); p++) {
                BytesRef blob = rootBlobs.getBytesRef(rootBlobs.getFirstValueIndex(p), scratch);
                FieldExtract.process(builder, blob, KEY_BYTES);
            }
            try (BytesRefBlock extracted = builder.build()) {
                BytesRef valueScratch = new BytesRef();
                for (int p = 0; p < extracted.getPositionCount(); p++) {
                    if (extracted.isNull(p)) {
                        throw new AssertionError("unexpected null at position [" + p + "]");
                    }
                    sum += Integer.parseInt(extracted.getBytesRef(extracted.getFirstValueIndex(p), valueScratch).utf8ToString());
                }
            }
        }
        return sum;
    }

    @Setup
    public void setup() throws IOException {
        setupIndex();
        setupPages();
    }

    private void setupIndex() throws IOException {
        IndexSettings indexSettings = defaultIndexSettings();
        fieldType = new FlattenedFieldMapper.Builder(FIELD, indexSettings).build(MapperBuilderContext.root(false, false)).fieldType();

        directory = new ByteBuffersDirectory();
        try (IndexWriter iw = new IndexWriter(directory, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
            for (int i = 0; i < indexSize; i++) {
                List<SortedSetDocValuesField> doc = new ArrayList<>(subFields);
                String value = Integer.toString(i % VALUE_MOD);
                for (int k = 0; k < subFields; k++) {
                    // "key\0value" is exactly how FlattenedFieldParser stores each leaf in the keyed channel.
                    doc.add(new SortedSetDocValuesField(KEYED_FIELD, new BytesRef("sub_" + k + SEPARATOR + value)));
                }
                iw.addDocument(doc);
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
                        for (int doc = begin; doc < end; doc++) {
                            docs.appendInt(doc);
                        }
                        pages.add(
                            new Page(
                                new DocVector(
                                    AlwaysReferencedIndexedByShardId.INSTANCE,
                                    blockFactory.newConstantIntBlockWith(0, end - begin).asVector(),
                                    blockFactory.newConstantIntBlockWith(ctx.ord, end - begin).asVector(),
                                    docs.build(),
                                    DocVector.config().singleSegmentNonDecreasing(true)
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
                                        AlwaysReferencedIndexedByShardId.INSTANCE,
                                        blockFactory.newConstantIntVector(0, size),
                                        leafs.build(),
                                        docs.build(),
                                        DocVector.config()
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
                                AlwaysReferencedIndexedByShardId.INSTANCE,
                                blockFactory.newConstantIntBlockWith(0, size).asVector(),
                                leafs.build().asBlock().asVector(),
                                docs.build(),
                                DocVector.config()
                            ).asBlock()
                        )
                    );
                }
            }
            default -> throw new IllegalArgumentException("unsupported layout [" + layout + "]");
        }
    }

    @TearDown
    public void teardownIndex() throws IOException {
        IOUtils.close(reader, directory);
    }

    private static class BenchContext implements MappedFieldType.BlockLoaderContext {
        private final BlockLoaderFunctionConfig functionConfig;

        BenchContext(BlockLoaderFunctionConfig functionConfig) {
            this.functionConfig = functionConfig;
        }

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

        @Override
        public MappingLookup mappingLookup() {
            return null;
        }

        @Override
        public BlockLoaderFunctionConfig blockLoaderFunctionConfig() {
            return functionConfig;
        }

        @Override
        public Warnings warnings() {
            return null;
        }

        @Override
        public ByteSizeValue ordinalsByteSize() {
            return DEFAULT_ORDINALS_BYTE_SIZE;
        }

        @Override
        public ByteSizeValue scriptByteSize() {
            return DEFAULT_SCRIPT_BYTE_SIZE;
        }
    }
}
