/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.mapper;

import org.apache.lucene.document.column.Column;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.bulk.BulkItemRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfColumn;
import org.elasticsearch.escf.EscfEncoder;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.IndexOperationBatch;
import org.elasticsearch.index.mapper.BatchMappingContext;
import org.elasticsearch.index.mapper.ColumnGroupResolver;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.IgnoredSourceFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.ShardBatchMapper;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.sourcebatch.MappedColumns;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.XContentType;
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
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Measures the columnar batch indexing path ({@code ShardBatchMapper#mapColumnBatch} minus the engine) for a {@code columnar}
 * index, with {@code synthetic} source as the baseline and {@code columnar_stored} source as the mode under test. The only
 * difference between the two modes on this path is {@code SourceFieldMapper#postColumnarParse}, which for {@code columnar_stored}
 * builds the whole-document {@code _source} blob for every row. The gap between the two modes is therefore the cost of that blob,
 * and it is the number that the direct-write work (elastic/elasticsearch#161525) must shrink.
 *
 * <p>Three benchmark methods cover the stages of the path, so that a change can be located:
 * <ul>
 *   <li>{@link #mapBatches}: the mapper stage only, on batches that were ESCF-encoded in {@code @Setup}. This isolates the field
 *       mappers and the metadata hooks, including the blob.</li>
 *   <li>{@link #encodeAndMapBatches}: ESCF encoding plus the mapper stage, which is everything the data node does before the
 *       engine, in {@code ShardBatchIndexer} chunk sizes.</li>
 *   <li>{@link #parseRows}: the row path ({@code DocumentMapper#parse}) over the same documents, for reference against the
 *       batch path and against {@link ColumnarIndexingThroughputBenchmark}.</li>
 * </ul>
 *
 * <p>Each invocation processes {@code docCount} documents, so the reported average time is per {@code docCount} documents. The
 * {@code mapping} parameter chooses the field mix: {@code keyword_long} covers the types that the first stage of the direct writer
 * supports, {@code mixed} adds the types of the later stages, and {@code mixed_flattened} adds a {@code flattened} field and a
 * {@code text} field, the column group and the text family of the last stages.
 *
 * <p>The setup refuses a mapping and document set that the batch path would fall back on, so that the benchmark never measures
 * the row path silently.
 */
@Fork(value = 1)
@Warmup(iterations = 2)
@Measurement(iterations = 3)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class ColumnarStoredSourceBatchIndexingBenchmark {

    private static final String INDEX_NAME = "benchmark-index";

    /** Field types of the first stage of the direct source writer: keyword and long, scalars and leaf arrays. */
    private static final String KEYWORD_LONG_MAPPING = """
        {
          "_doc": {
            "dynamic": "strict",
            "properties": {
              "host":       { "type": "keyword" },
              "service":    { "type": "keyword" },
              "message_id": { "type": "keyword" },
              "tags":       { "type": "keyword" },
              "count":      { "type": "long" },
              "bytes":      { "type": "long" },
              "latencies":  { "type": "long" }
            }
          }
        }
        """;

    /** The same fields plus the types of the later stages. */
    private static final String MIXED_MAPPING = """
        {
          "_doc": {
            "dynamic": "strict",
            "properties": {
              "host":       { "type": "keyword" },
              "service":    { "type": "keyword" },
              "message_id": { "type": "keyword" },
              "tags":       { "type": "keyword" },
              "count":      { "type": "long" },
              "bytes":      { "type": "long" },
              "latencies":  { "type": "long" },
              "cpu":        { "type": "double" },
              "@timestamp": { "type": "date" },
              "client_ip":  { "type": "ip" },
              "enabled":    { "type": "boolean" }
            }
          }
        }
        """;

    /** {@link #MIXED_MAPPING} plus the column group and the text family. */
    private static final String MIXED_FLATTENED_MAPPING = """
        {
          "_doc": {
            "dynamic": "strict",
            "properties": {
              "host":       { "type": "keyword" },
              "service":    { "type": "keyword" },
              "message_id": { "type": "keyword" },
              "tags":       { "type": "keyword" },
              "count":      { "type": "long" },
              "bytes":      { "type": "long" },
              "latencies":  { "type": "long" },
              "cpu":        { "type": "double" },
              "@timestamp": { "type": "date" },
              "client_ip":  { "type": "ip" },
              "enabled":    { "type": "boolean" },
              "message":    { "type": "text" },
              "attrs":      { "type": "flattened" }
            }
          }
        }
        """;

    private static final String[] HOSTS = { "host-1", "host-2", "host-3", "host-4", "host-5" };
    private static final String[] SERVICES = { "frontend", "checkout", "payments", "search", "auth" };
    private static final String[] TAGS = { "prod", "canary", "eu-west", "us-east", "slow", "retry" };

    /**
     * Source mode of the index. {@code synthetic} is the baseline: {@code postColumnarParse} writes nothing. {@code columnar_stored}
     * builds the whole-document blob for every row.
     */
    @Param({ "synthetic", "columnar_stored" })
    private String sourceMode;

    /** Field mix of the mapping and the generated documents. */
    @Param({ "keyword_long", "mixed", "mixed_flattened" })
    private String mapping;

    /** Number of documents processed per benchmark invocation. */
    @Param({ "10000" })
    private int docCount;

    /** Documents per batch; {@code ShardBatchIndexer#BATCH_CHUNK_SIZE} is 5000. */
    @Param({ "5000" })
    private int batchSize;

    /** Seed for deterministic document generation across runs. */
    @Param({ "1600172297" })
    private long seed;

    private MapperService mapperService;
    private MappingLookup mappingLookup;
    private IndexSettings indexSettings;
    private MetadataFieldMapper[] metadataMappers;
    private Recycler<BytesRef> recycler;

    /** One entry per document, shared by every path. */
    private BulkItemRequest[] items;
    private SourceToParse[] rowSources;

    /** Per chunk, built once in setup for {@link #mapBatches}. */
    private EscfBatch[] encodedChunks;
    private IndexOperationBatch[] operationChunks;
    private ShardBatchMapper.BatchMapperResolution[] resolutions;

    @Setup
    public void setUp() throws IOException {
        BenchmarkLogging.configure();

        assert IndexMode.COLUMNAR.supportedSourceModes()
            .stream()
            .map(mode -> mode.toString().toLowerCase(Locale.ROOT))
            .toList()
            .contains(sourceMode);

        final String mappingJson = switch (mapping) {
            case "keyword_long" -> KEYWORD_LONG_MAPPING;
            case "mixed" -> MIXED_MAPPING;
            case "mixed_flattened" -> MIXED_FLATTENED_MAPPING;
            default -> throw new IllegalArgumentException("unknown mapping [" + mapping + "]");
        };
        final Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), sourceMode)
            .build();
        mapperService = MapperServiceFactory.create(mappingJson, List.of(), settings);
        mappingLookup = mapperService.mappingLookup();
        indexSettings = mapperService.getIndexSettings();
        metadataMappers = mappingLookup.getMapping().getSortedMetadataMappers();
        // The same recycler type the data node hands to ShardBatchIndexer (BigArrays#bytesRefRecycler).
        recycler = new BytesRefRecycler(new PageCacheRecycler(Settings.EMPTY));

        final Random random = new Random(seed);
        final boolean mixed = "keyword_long".equals(mapping) == false;
        final boolean flattened = "mixed_flattened".equals(mapping);
        items = new BulkItemRequest[docCount];
        rowSources = new SourceToParse[docCount];
        for (int i = 0; i < docCount; i++) {
            final String id = UUIDs.randomBase64UUID(random);
            final BytesReference source = new BytesArray(generateDoc(random, mixed, flattened).getBytes(StandardCharsets.UTF_8));
            items[i] = new BulkItemRequest(i, new IndexRequest(INDEX_NAME).id(id).source(source, XContentType.JSON));
            rowSources[i] = new SourceToParse(id, source, XContentType.JSON);
        }

        final int chunkCount = (docCount + batchSize - 1) / batchSize;
        encodedChunks = new EscfBatch[chunkCount];
        operationChunks = new IndexOperationBatch[chunkCount];
        resolutions = new ShardBatchMapper.BatchMapperResolution[chunkCount];
        try (EscfEncoder encoder = new EscfEncoder(recycler)) {
            for (int chunk = 0; chunk < chunkCount; chunk++) {
                final int from = chunk * batchSize;
                final int to = Math.min(from + batchSize, docCount);
                encodedChunks[chunk] = encodeChunk(encoder, from, to, chunk);
                operationChunks[chunk] = operationBatch(from, to, encodedChunks[chunk]);
                resolutions[chunk] = resolve(encodedChunks[chunk]);
            }
        }

        // Fail loudly rather than measure a batch the mappers refuse: ShardBatchMapper would fall back to the row path in production.
        final long columns = mapChunk(encodedChunks[0], operationChunks[0], resolutions[0], true);
        if (columns == 0) {
            throw new IllegalStateException("the batch path produced no columns for mapping [" + mapping + "]");
        }
    }

    @TearDown
    public void tearDown() {
        for (EscfBatch chunk : encodedChunks) {
            chunk.close();
        }
    }

    /**
     * Maps the pre-encoded batches: every field mapper and metadata hook, including {@code SourceFieldMapper#postColumnarParse}.
     * Returns the number of Lucene columns produced, so that JMH cannot eliminate the work.
     */
    @Benchmark
    public long mapBatches() throws IOException {
        long columns = 0;
        for (int chunk = 0; chunk < encodedChunks.length; chunk++) {
            columns += mapChunk(encodedChunks[chunk], operationChunks[chunk], resolutions[chunk], false);
        }
        return columns;
    }

    /**
     * ESCF-encodes the documents in chunks and maps each chunk, as {@code ShardBatchIndexer} does before it hands the batch to the
     * engine. Resolution is repeated per chunk as well, since production resolves per chunk.
     */
    @Benchmark
    public long encodeAndMapBatches() throws IOException {
        long columns = 0;
        try (EscfEncoder encoder = new EscfEncoder(recycler)) {
            for (int chunk = 0; chunk < encodedChunks.length; chunk++) {
                final int from = chunk * batchSize;
                final int to = Math.min(from + batchSize, docCount);
                try (EscfBatch escfBatch = encodeChunk(encoder, from, to, chunk)) {
                    columns += mapChunk(escfBatch, operationBatch(from, to, escfBatch), resolve(escfBatch), false);
                }
            }
        }
        return columns;
    }

    /** The row path over the same documents, for reference. Returns the number of Lucene documents produced. */
    @Benchmark
    public long parseRows() {
        long total = 0;
        for (SourceToParse source : rowSources) {
            total += mapperService.documentMapper().parse(source).docs().size();
        }
        return total;
    }

    private EscfBatch encodeChunk(EscfEncoder encoder, int from, int to, int partition) throws IOException {
        for (int i = from; i < to; i++) {
            final IndexRequest request = (IndexRequest) items[i].request();
            encoder.addDocument(request.source(), XContentType.JSON, partition);
        }
        return encoder.buildPartition(partition);
    }

    private IndexOperationBatch operationBatch(int from, int to, EscfBatch escfBatch) {
        return IndexOperationBatch.initFromBulk(items, from, to, escfBatch, Engine.Operation.Origin.PRIMARY, 1L, 0L);
    }

    private ShardBatchMapper.BatchMapperResolution resolve(EscfBatch escfBatch) {
        final ShardBatchMapper.BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(escfBatch, mappingLookup, indexSettings);
        if (resolution == null) {
            throw new IllegalStateException(
                "ShardBatchMapper.resolveMappers refused mapping [" + mapping + "]; the batch path would fall back to the row path"
            );
        }
        return resolution;
    }

    /**
     * The mapper stage of {@code ShardBatchMapper#mapColumnBatch}: metadata pre-hooks, leaf and group field mappers, metadata
     * post-hooks, then the column batch the engine would receive. Exceptions propagate, where production would fall back, so that
     * a refused batch fails the benchmark instead of being measured on the wrong path.
     */
    private long mapChunk(
        EscfBatch escfBatch,
        IndexOperationBatch operationBatch,
        ShardBatchMapper.BatchMapperResolution resolution,
        boolean verify
    ) throws IOException {
        try (BatchMappingContext ctx = new BatchMappingContext(operationBatch, mappingLookup, indexSettings, recycler)) {
            for (MetadataFieldMapper metadataMapper : metadataMappers) {
                metadataMapper.preColumnarParse(ctx);
            }
            final FieldMapper[] columnMappers = resolution.columnMappers();
            for (int c = 0; c < columnMappers.length; c++) {
                if (columnMappers[c] != null) {
                    columnMappers[c].mapColumnBatch(ctx, escfBatch.column(c));
                }
            }
            for (ColumnGroupResolver.ColumnGroupResolution group : resolution.columnGroups()) {
                final int[] leafIndexes = group.leafIndexes();
                final EscfColumn[] groupColumns = new EscfColumn[leafIndexes.length];
                for (int i = 0; i < leafIndexes.length; i++) {
                    groupColumns[i] = escfBatch.column(leafIndexes[i]);
                }
                group.mapper().mapColumnGroupBatch(ctx, groupColumns, group.relativeKeys());
            }
            for (MetadataFieldMapper metadataMapper : metadataMappers) {
                metadataMapper.postColumnarParse(ctx);
            }
            final MappedColumns mappedColumns = ctx.columns();
            long columns = 0;
            boolean blobPresent = false;
            for (Column column : mappedColumns.toColumnBatch().columns()) {
                columns++;
                blobPresent |= IgnoredSourceFieldMapper.NAME.equals(column.name());
            }
            if (verify && "columnar_stored".equals(sourceMode) && blobPresent == false) {
                throw new IllegalStateException("columnar_stored source produced no [" + IgnoredSourceFieldMapper.NAME + "] blob column");
            }
            return columns;
        }
    }

    /**
     * One log-like document. Values differ per document so that the string columns do not collapse into a handful of dictionary
     * entries, and two fields are leaf arrays, since array order is the part of the blob contract that costs the most to keep. The
     * flattened object lists its keys in one order, as the batch path writes its slots in the order the schema first saw the keys.
     */
    private static String generateDoc(Random random, boolean mixed, boolean flattened) {
        final StringBuilder doc = new StringBuilder(256);
        doc.append("{\"host\":\"").append(HOSTS[random.nextInt(HOSTS.length)]).append('"');
        doc.append(",\"service\":\"").append(SERVICES[random.nextInt(SERVICES.length)]).append('"');
        doc.append(",\"message_id\":\"").append(UUIDs.randomBase64UUID(random)).append('"');
        doc.append(",\"tags\":[");
        final int tagCount = random.nextInt(1, 4);
        for (int t = 0; t < tagCount; t++) {
            if (t > 0) {
                doc.append(',');
            }
            doc.append('"').append(TAGS[random.nextInt(TAGS.length)]).append('"');
        }
        doc.append(']');
        doc.append(",\"count\":").append(random.nextInt(1000));
        doc.append(",\"bytes\":").append((long) (random.nextDouble() * 10_000_000L));
        doc.append(",\"latencies\":[");
        final int latencyCount = random.nextInt(2, 5);
        for (int l = 0; l < latencyCount; l++) {
            if (l > 0) {
                doc.append(',');
            }
            doc.append(random.nextInt(1, 5000));
        }
        doc.append(']');
        if (mixed) {
            doc.append(",\"cpu\":").append(String.format(Locale.ROOT, "%.6f", random.nextDouble()));
            doc.append(
                String.format(
                    Locale.ROOT,
                    ",\"@timestamp\":\"2024-01-%02dT%02d:%02d:%02dZ\"",
                    random.nextInt(1, 29),
                    random.nextInt(24),
                    random.nextInt(60),
                    random.nextInt(60)
                )
            );
            doc.append(
                String.format(
                    Locale.ROOT,
                    ",\"client_ip\":\"%d.%d.%d.%d\"",
                    random.nextInt(256),
                    random.nextInt(256),
                    random.nextInt(256),
                    random.nextInt(256)
                )
            );
            doc.append(",\"enabled\":").append(random.nextBoolean());
        }
        if (flattened) {
            doc.append(",\"message\":\"request ").append(UUIDs.randomBase64UUID(random)).append(" served by ");
            doc.append(SERVICES[random.nextInt(SERVICES.length)]).append(" in ").append(random.nextInt(1, 5000)).append(" ms\"");
            doc.append(",\"attrs\":{\"region\":\"").append(TAGS[random.nextInt(TAGS.length)]).append('"');
            doc.append(",\"k8s\":{\"pod\":\"pod-").append(random.nextInt(100)).append("\",\"node\":\"");
            doc.append(HOSTS[random.nextInt(HOSTS.length)]).append("\"}");
            doc.append(",\"labels\":[");
            final int labelCount = random.nextInt(1, 4);
            for (int l = 0; l < labelCount; l++) {
                if (l > 0) {
                    doc.append(',');
                }
                doc.append('"').append(TAGS[random.nextInt(TAGS.length)]).append('"');
            }
            doc.append("],\"retries\":").append(random.nextInt(5)).append('}');
        }
        return doc.append('}').toString();
    }
}
