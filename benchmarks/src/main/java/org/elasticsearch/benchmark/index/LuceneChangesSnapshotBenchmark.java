/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.benchmark.index;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.FSDirectory;
import org.elasticsearch.benchmark.index.mapper.MapperServiceFactory;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.codec.PerFieldMapperCodec;
import org.elasticsearch.index.codec.zstd.Zstd814StoredFieldsFormat;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.LuceneChangesSnapshot;
import org.elasticsearch.index.engine.LuceneChangesSnapshotNew;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
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
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

@Fork(value = 1)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class LuceneChangesSnapshotBenchmark {
    @Param({ "default", "logsdb" })
    String indexMode;

    @Param({ "true", "false" })
    boolean sequential;

    static final int NUM_OPS = 10000;

    Path path;
    MapperService mapperService;
    FSDirectory dir;
    IndexReader reader;
    Engine.Searcher searcher;

    static {
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    }

    @Setup
    public void setup() throws IOException {
        this.path = Files.createTempDirectory("snapshot_changes");

        Settings settings = indexMode.equals("logsdb") ? Settings.builder().put("index.mode", "logsdb").build() : Settings.EMPTY;
        this.mapperService = MapperServiceFactory.create(settings, readMappings());
        IndexWriterConfig config = new IndexWriterConfig();
        config.setCodec(
            new PerFieldMapperCodec(Zstd814StoredFieldsFormat.Mode.BEST_COMPRESSION, mapperService, BigArrays.NON_RECYCLING_INSTANCE)
        );
        if (sequential == false) {
            config.setIndexSort(new Sort(new SortField[] { new SortField("_seq_no", SortField.Type.LONG, true) }));
        }
        try (FSDirectory dir = FSDirectory.open(path); IndexWriter writer = new IndexWriter(dir, config);) {
            try (
                var inputStream = readSampleDocs();
                XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, inputStream)
            ) {
                int id = 0;
                // find the hits array
                while (parser.nextToken() != XContentParser.Token.START_ARRAY) {
                    parser.nextToken();
                }

                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                    // skip start object
                    parser.nextToken();
                    // skip _source field name
                    parser.nextToken();
                    XContentBuilder source = XContentBuilder.builder(XContentType.JSON.xContent());
                    source.copyCurrentStructure(parser);
                    var sourceBytes = BytesReference.bytes(source);
                    SourceToParse sourceToParse = new SourceToParse(Integer.toString(id), sourceBytes, XContentType.JSON);
                    ParsedDocument doc = mapperService.documentMapper().parse(sourceToParse);
                    doc.updateSeqID(id, 0);
                    doc.version().setLongValue(0);
                    writer.addDocuments(doc.docs());
                    id++;
                    parser.nextToken();
                }
            }
        }
        this.dir = FSDirectory.open(path);
        this.reader = DirectoryReader.open(dir);
        this.searcher = new Engine.Searcher("snapshot", reader, new BM25Similarity(), null, new QueryCachingPolicy() {
            @Override
            public void onUse(Query query) {}

            @Override
            public boolean shouldCache(Query query) throws IOException {
                return false;
            }
        }, () -> {});
    }

    @TearDown
    public void tearDown() {
        try {
            for (var file : dir.listAll()) {
                dir.deleteFile(file);
            }
            Files.delete(path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        IOUtils.closeWhileHandlingException(searcher, reader, dir);
    }

    @Benchmark
    @OperationsPerInvocation(NUM_OPS)
    public long recover() throws IOException {
        long totalSize = 0;
        Translog.Snapshot snapshot = mapperService.mappingLookup().isSourceSynthetic()
            ? new LuceneChangesSnapshotNew(
                mapperService.mappingLookup().newSourceLoader(mapperService.getMapperMetrics().sourceFieldMetrics()),
                searcher,
                512,
                0,
                NUM_OPS - 1,
                true,
                true,
                true,
                IndexVersion.current()
            )
            : new LuceneChangesSnapshot(
                searcher,
                LuceneChangesSnapshot.DEFAULT_BATCH_SIZE,
                0,
                NUM_OPS - 1,
                true,
                true,
                true,
                IndexVersion.current()
            );
        try (snapshot) {
            Translog.Operation op;
            while ((op = snapshot.next()) != null) {
                totalSize += op.estimateSize();
            }
        }
        return totalSize;
    }

    private String readMappings() throws IOException {
        return Streams.readFully(LuceneChangesSnapshotBenchmark.class.getResourceAsStream("lucene-changes-mappings.json")).utf8ToString();
    }

    private InputStream readSampleDocs() throws IOException {
        return new GZIPInputStream(LuceneChangesSnapshotBenchmark.class.getResourceAsStream("lucene-changes-sample-docs.json.gz"));
    }
}
