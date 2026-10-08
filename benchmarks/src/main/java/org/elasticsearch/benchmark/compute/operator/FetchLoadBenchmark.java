/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.compute.operator;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.benchmark.index.mapper.MapperServiceFactory;
import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.AlwaysReferencedIndexedByShardId;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromSingleton;
import org.elasticsearch.compute.lucene.read.FetchDocsSourceOperator;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceFieldMetrics;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.index.mapper.blockloader.Warnings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.lookup.SourceFilter;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
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
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;

/**
 * Loads the columns of some documents of one shard, the work the fetch driver of one shard does for a fetch request.
 * <p>
 * {@code fetch} feeds the loader what {@link FetchDocsSourceOperator} emits: one page per segment run, the documents
 * in order, so the loader reads each segment once and sequentially. {@code shuffled} feeds it the same documents in
 * one page in random order, the shape the remote fetch prototype and the late materialization of the query phase give
 * it. The time of one operation covers the fixed cost of a request, like opening the readers of each segment, and the
 * cost per document, so the slope over {@code docs} is the cost per document.
 * <p>
 * The index looks like a log: a timestamp, two keywords, a status code and a message of about 200 bytes, in about 30
 * segments. {@code source} is the whole {@code _source}, what the Rally ES|QL queries return. {@code keywords} are
 * doc values. {@code message} is a text field, which loads from {@code _source}.
 * <p>
 * {@code random} documents are spread over the whole index, like the best matches of a full text query. {@code newest}
 * are the last documents indexed, like the winners of {@code SORT @timestamp DESC} on logs indexed in time order. They
 * sit next to each other in the newest segments, dense enough for the loader to read stored fields sequentially.
 */
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 7, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Fork(1)
public class FetchLoadBenchmark {
    static {
        // BlockFactory needs logging before its class initializes
        BenchmarkLogging.configure();
    }

    private static final BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(NoopCircuitBreaker.INSTANCE)
        .build();

    /** About 30 segments, a shard that took writes for a while. */
    private static final int SEGMENTS = 30;
    /** The page size of the fetch drivers for small rows. */
    private static final int MAX_PAGE_SIZE = 1024;
    private static final String[] WORDS = {
        "connection",
        "timeout",
        "request",
        "served",
        "cache",
        "miss",
        "retry",
        "upstream",
        "closed",
        "slow" };

    /**
     * Both shapes must load the same values for every column and source mode the benchmark measures. Builds a smaller
     * index than the benchmark, it runs in a unit test.
     */
    static void selfTest() {
        for (String sourceMode : new String[] { "stored", "synthetic" }) {
            FetchLoadBenchmark benchmark = new FetchLoadBenchmark();
            benchmark.indexSize = 10_000;
            benchmark.docs = 100;
            benchmark.sourceMode = sourceMode;
            benchmark.columns = "source";
            try {
                benchmark.setupIndex();
                for (String columns : new String[] { "source", "keywords", "message" }) {
                    for (String distribution : new String[] { "random", "newest" }) {
                        benchmark.fields = benchmark.fields(columns);
                        benchmark.distribution = distribution;
                        long fetch = benchmark.checksumOf("fetch");
                        long shuffled = benchmark.checksumOf("shuffled");
                        if (fetch != shuffled || fetch == 0) {
                            throw new AssertionError(
                                "["
                                    + sourceMode
                                    + "]["
                                    + columns
                                    + "]["
                                    + distribution
                                    + "] fetch loaded ["
                                    + fetch
                                    + "] but shuffled ["
                                    + shuffled
                                    + "]"
                            );
                        }
                    }
                }
            } finally {
                benchmark.teardownIndex();
            }
        }
    }

    /**
     * Loads the documents of the first iteration in {@code shape}.
     */
    private long checksumOf(String shape) {
        this.shape = shape;
        seed = 1;
        chooseDocs();
        return load();
    }

    @Param({ "20", "100", "500" })
    public int docs;

    @Param({ "source", "keywords", "message" })
    public String columns;

    @Param({ "fetch", "shuffled" })
    public String shape;

    @Param({ "stored", "synthetic" })
    public String sourceMode;

    @Param({ "random", "newest" })
    public String distribution;

    private int indexSize = 100_000;
    /** Each iteration loads other documents, the same for both shapes. */
    private long seed = 1;
    private Path path;
    private Directory directory;
    private DirectoryReader reader;
    private MapperService mapperService;
    private List<ValuesSourceReaderOperator.FieldInfo> fields;
    /** The documents to load, sorted by segment and doc, as the coordinator asks for them. */
    private int[] segments;
    private int[] docIds;
    /** The order of the documents in the {@code shuffled} page. */
    private int[] shuffle;

    @Setup(Level.Trial)
    public void setupIndex() {
        try {
            mapperService = MapperServiceFactory.create("""
                {
                  "_doc": {
                    "properties": {
                      "@timestamp": { "type": "date" },
                      "host": { "properties": { "name": { "type": "keyword" } } },
                      "service": { "type": "keyword" },
                      "status": { "type": "long" },
                      "message": { "type": "text" }
                    }
                  }
                }
                """, List.of(), Settings.builder().put("index.mapping.source.mode", sourceMode).build());
            path = Files.createTempDirectory("fetch-load");
            directory = FSDirectory.open(path);
            Random random = new Random(7);
            int commitInterval = indexSize / SEGMENTS;
            try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
                for (int i = 0; i < indexSize; i++) {
                    StringBuilder message = new StringBuilder();
                    for (int w = 0; w < 25; w++) {
                        message.append(w == 0 ? "" : " ").append(WORDS[random.nextInt(WORDS.length)]);
                    }
                    String json = String.format(
                        Locale.ROOT,
                        """
                            {"@timestamp": %d, "host": {"name": "host-%d"}, "service": "service-%d", "status": %d, "message": "%s"}""",
                        1_700_000_000_000L + i * 1000L,
                        i % 50,
                        i % 7,
                        new int[] { 200, 200, 200, 404, 500 }[random.nextInt(5)],
                        message
                    );
                    ParsedDocument parsed = mapperService.documentMapper()
                        .parse(new SourceToParse(Integer.toString(i), new BytesArray(json), XContentType.JSON));
                    writer.addDocument(parsed.rootDoc());
                    if ((i + 1) % commitInterval == 0) {
                        writer.commit();
                    }
                }
                writer.commit();
            }
            // the engine wraps its readers like this, and only a wrapped leaf reads stored fields sequentially
            reader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(directory), new ShardId("benchmark", "_na_", 0));
            fields = fields(columns);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * New documents each iteration, so no iteration measures a lucky spread over the segments.
     */
    @Setup(Level.Iteration)
    public void chooseDocs() {
        Random random = new Random(seed++);
        long[] chosen = switch (distribution) {
            case "random" -> random.longs(0, indexSize).distinct().limit(docs).toArray();
            case "newest" -> LongStream.range(indexSize - docs, indexSize).toArray();
            default -> throw new IllegalArgumentException("unknown distribution [" + distribution + "]");
        };
        Arrays.sort(chosen);
        segments = new int[docs];
        docIds = new int[docs];
        for (int d = 0; d < docs; d++) {
            int global = (int) chosen[d];
            LeafReaderContext leaf = reader.leaves().get(ReaderUtil.subIndex(global, reader.leaves()));
            segments[d] = leaf.ord;
            docIds[d] = global - leaf.docBase;
        }
        shuffle = random.ints(0, docs).distinct().limit(docs).toArray();
    }

    @TearDown(Level.Trial)
    public void teardownIndex() {
        try {
            IOUtils.close(reader, directory, mapperService);
            if (path != null) {
                IOUtils.rm(path);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Benchmark
    public long load() {
        DriverContext driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null);
        long checksum = 0;
        try (
            ValuesSourceReaderOperator loader = new ValuesSourceReaderOperator(
                driverContext,
                ByteSizeValue.ofMb(1).getBytes(),
                fields,
                new IndexedByShardIdFromSingleton<>(
                    new ValuesSourceReaderOperator.ShardContext(
                        reader,
                        this::newSourceLoader,
                        EsqlPlugin.STORED_FIELDS_SEQUENTIAL_PROPORTION.getDefault(Settings.EMPTY)
                    )
                ),
                fields.size() <= PlannerSettings.REUSE_COLUMN_LOADERS_THRESHOLD.get(Settings.EMPTY),
                0,
                PlannerSettings.SOURCE_RESERVATION_FACTOR.getDefault(Settings.EMPTY),
                PlannerSettings.DOC_SEQUENCE_BYTES_REF_FIELD_THRESHOLD.getDefault(Settings.EMPTY),
                () -> 0L
            )
        ) {
            for (Page page : pages()) {
                loader.addInput(page);
                for (Page loaded = loader.getOutput(); loaded != null; loaded = loader.getOutput()) {
                    checksum += checksum(loaded);
                    loaded.releaseBlocks();
                }
            }
        }
        return checksum;
    }

    private List<Page> pages() {
        List<Page> pages = new ArrayList<>();
        switch (shape) {
            case "fetch" -> {
                FetchDocsSourceOperator source = new FetchDocsSourceOperator(
                    blockFactory,
                    AlwaysReferencedIndexedByShardId.INSTANCE,
                    new FetchDocsSourceOperator.ShardDocs(0, segments, docIds),
                    MAX_PAGE_SIZE
                );
                while (source.isFinished() == false) {
                    Page page = source.getOutput();
                    if (page != null) {
                        pages.add(page);
                    }
                }
            }
            case "shuffled" -> {
                try (
                    IntVector.FixedBuilder segmentBuilder = blockFactory.newIntVectorFixedBuilder(docs);
                    IntVector.FixedBuilder docBuilder = blockFactory.newIntVectorFixedBuilder(docs)
                ) {
                    for (int d : shuffle) {
                        segmentBuilder.appendInt(segments[d]);
                        docBuilder.appendInt(docIds[d]);
                    }
                    pages.add(
                        new Page(
                            new DocVector(
                                AlwaysReferencedIndexedByShardId.INSTANCE,
                                blockFactory.newConstantIntVector(0, docs),
                                segmentBuilder.build(),
                                docBuilder.build(),
                                DocVector.config()
                            ).asBlock()
                        )
                    );
                }
            }
            default -> throw new IllegalArgumentException("unknown shape [" + shape + "]");
        }
        return pages;
    }

    /**
     * Adds up the loaded values, the same for both shapes because it doesn't depend on their order.
     */
    private static long checksum(Page page) {
        long sum = 0;
        BytesRef scratch = new BytesRef();
        for (int b = 1; b < page.getBlockCount(); b++) {
            Block block = page.getBlock(b);
            for (int p = 0; p < block.getPositionCount(); p++) {
                if (block.isNull(p)) {
                    continue;
                }
                int first = block.getFirstValueIndex(p);
                for (int v = first; v < first + block.getValueCount(p); v++) {
                    sum += switch (block) {
                        case BytesRefBlock bytes -> bytes.getBytesRef(v, scratch).hashCode();
                        case LongBlock longs -> Long.hashCode(longs.getLong(v));
                        default -> throw new IllegalArgumentException("unexpected block [" + block + "]");
                    };
                }
            }
        }
        return sum;
    }

    private List<ValuesSourceReaderOperator.FieldInfo> fields(String columns) {
        return switch (columns) {
            case "source" -> List.of(field("_source", ElementType.BYTES_REF));
            case "keywords" -> List.of(
                field("host.name", ElementType.BYTES_REF),
                field("service", ElementType.BYTES_REF),
                field("status", ElementType.LONG)
            );
            case "message" -> List.of(field("message", ElementType.BYTES_REF));
            default -> throw new IllegalArgumentException("unknown columns [" + columns + "]");
        };
    }

    private ValuesSourceReaderOperator.FieldInfo field(String name, ElementType type) {
        MappedFieldType fieldType = mapperService.fieldType(name);
        if (fieldType == null) {
            throw new IllegalArgumentException("no field [" + name + "]");
        }
        return new ValuesSourceReaderOperator.FieldInfo(
            name,
            type,
            false,
            (driverContext, shard) -> ValuesSourceReaderOperator.load(fieldType.blockLoader(new Context()))
        );
    }

    /**
     * Builds the source loader the way the data node does, filtered to the paths the fields read.
     */
    private SourceLoader newSourceLoader(Set<String> sourcePaths) {
        SourceFilter filter = sourcePaths == null ? null : new SourceFilter(sourcePaths.toArray(String[]::new), null);
        return mapperService.mappingLookup().newSourceLoader(filter, SourceFieldMetrics.NOOP, null);
    }

    /**
     * What the mappers ask while they build their loaders, answered from the mapping.
     */
    private class Context implements MappedFieldType.BlockLoaderContext {
        @Override
        public String indexName() {
            return "benchmark";
        }

        @Override
        public IndexSettings indexSettings() {
            return mapperService.getIndexSettings();
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
            return mapperService.mappingLookup().sourcePaths(name);
        }

        @Override
        public String parentField(String field) {
            return mapperService.mappingLookup().parentField(field);
        }

        @Override
        public FieldNamesFieldMapper.FieldNamesFieldType fieldNames() {
            return FieldNamesFieldMapper.FieldNamesFieldType.get(true);
        }

        @Override
        public MappingLookup mappingLookup() {
            return mapperService.mappingLookup();
        }

        @Override
        public BlockLoaderFunctionConfig blockLoaderFunctionConfig() {
            return null;
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
