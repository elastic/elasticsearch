/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.tsdb;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.index.codec.Elasticsearch96Codec;
import org.elasticsearch.index.codec.tsdb.BinaryDVCompressionMode;
import org.elasticsearch.index.codec.tsdb.TSDBSyntheticIdPostingsFormat;
import org.elasticsearch.index.codec.tsdb.es819.ES819Version3TSDBDocValuesFormat;
import org.elasticsearch.index.mapper.SyntheticIdField;
import org.elasticsearch.index.mapper.TsidExtractingIdFieldMapper;
import org.elasticsearch.index.mapper.Uid;
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
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Measures resolving a synthetic {@code _id} to a document, the lookup every indexed document performs in a time series index.
 * <p>
 * {@code seekCeil} locates the {@code _tsid} and then the timestamp within it, and both steps walk documents. The walks are
 * bounded by the doc values skipper interval rather than by the {@code _tsid}, so the parameters sweep how many documents share a
 * {@code _tsid} on either side of that interval, and the segment size independently.
 * <p>
 * Probe order matters because the terms enum is reused across lookups, as {@code PerThreadIDVersionAndSeqNoLookup} does: ascending
 * probes only ever move forwards, while shuffled probes force the readers to rewind.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
public class TSDBSyntheticIdSeekBenchmark {

    private static final String SYNTHETIC_ID = "_id";
    private static final String TS_ID = "_tsid";
    private static final String TIMESTAMP = "@timestamp";
    private static final String TS_ROUTING_HASH = "_ts_routing_hash";
    private static final long BASE_TIMESTAMP = 1_735_689_600_000L;
    private static final int ROUTING = 42;
    private static final int PROBES = 4096;

    @Param({ "262144", "1048576" })
    private int totalDocs;

    @Param({ "256", "4096", "16384", "65536" })
    private int docsPerTsid;

    @Param({ "true", "false" })
    private boolean ascendingProbes;

    private Directory directory;
    private DirectoryReader reader;
    private LeafReader leaf;
    private List<BytesRef> probes;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        // The synthetic id producer logs segment details on open, which needs the logging SPI bound.
        BenchmarkLogging.configure();
        directory = FSDirectory.open(Files.createTempDirectory("tsdb-seek-"));
        final int tsids = Math.max(1, totalDocs / docsPerTsid);

        // Binary doc values are written uncompressed: zstd needs native access, which a plain JMH JVM does not have, and
        // compression of the binary column is not what is being measured here.
        final var docValuesFormat = new ES819Version3TSDBDocValuesFormat(
            4096,
            512,
            true,
            BinaryDVCompressionMode.NO_COMPRESS,
            false,
            7,
            false
        );
        final var syntheticIdPostingsFormat = new TSDBSyntheticIdPostingsFormat();
        final var codec = new Elasticsearch96Codec() {
            @Override
            public DocValuesFormat getDocValuesFormatForField(String field) {
                return docValuesFormat;
            }

            @Override
            public PostingsFormat getPostingsFormatForField(String field) {
                // _id has to name the synthetic id format, since merging rewrites the per-field format attribute.
                return SYNTHETIC_ID.equals(field) ? syntheticIdPostingsFormat : super.getPostingsFormatForField(field);
            }
        };
        final var config = new IndexWriterConfig();
        config.setCodec(codec);
        config.setIndexSort(
            new Sort(new SortField(TS_ID, SortField.Type.STRING, false), new SortedNumericSortField(TIMESTAMP, SortField.Type.LONG, true))
        );
        config.setMergePolicy(new LogByteSizeMergePolicy());
        config.setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH);
        config.setRAMBufferSizeMB(2048);

        final var routingHash = Uid.encodeId(org.elasticsearch.index.mapper.TimeSeriesRoutingHashFieldMapper.encode(ROUTING));
        final List<BytesRef> all = new ArrayList<>(totalDocs);
        try (var writer = new IndexWriter(directory, config)) {
            for (int t = 0; t < tsids; t++) {
                final BytesRef tsid = new BytesRef(String.format(Locale.ROOT, "tsid-%08d", t).getBytes(StandardCharsets.UTF_8));
                for (int d = 0; d < docsPerTsid; d++) {
                    final long timestamp = BASE_TIMESTAMP + d;
                    final BytesRef uid = Uid.encodeId(TsidExtractingIdFieldMapper.createSyntheticId(tsid, timestamp, ROUTING));
                    final var doc = new Document();
                    // Skip indexes on _tsid and @timestamp, as a time series index has them: without one, locating a _tsid
                    // falls back to scanning from document 0 and the seek measures that instead.
                    doc.add(SortedDocValuesField.indexedField(TS_ID, tsid));
                    doc.add(SortedNumericDocValuesField.indexedField(TIMESTAMP, timestamp));
                    doc.add(new SortedDocValuesField(TS_ROUTING_HASH, routingHash));
                    doc.add(new SyntheticIdField(uid));
                    writer.addDocument(doc);
                    all.add(uid);
                }
            }
            // A single segment keeps the measurement on the seek itself rather than on how many leaves are visited.
            writer.forceMerge(1);
        }

        reader = DirectoryReader.open(directory);
        leaf = reader.leaves().getFirst().reader();

        probes = new ArrayList<>(PROBES);
        final int stride = Math.max(1, all.size() / PROBES);
        for (int i = 0; i < all.size() && probes.size() < PROBES; i += stride) {
            probes.add(all.get(i));
        }
        if (ascendingProbes) {
            probes.sort(BytesRef::compareTo);
        } else {
            Collections.shuffle(probes, new Random(42));
        }

        // Every probe must resolve, otherwise the benchmark measures a scan that runs off the end of the _tsid instead of a
        // successful seek, which looks like a plausible number and is not one.
        final TermsEnum check = leaf.terms(SYNTHETIC_ID).iterator();
        for (BytesRef probe : probes) {
            if (check.seekCeil(probe) != TermsEnum.SeekStatus.FOUND) {
                throw new IllegalStateException("probe did not resolve to an existing term: " + probe);
            }
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        org.apache.lucene.util.IOUtils.close(reader, directory);
    }

    /** Resolves every probe through one terms enum, as the engine does per leaf per thread. */
    @Benchmark
    public void seekCeil(Blackhole bh) throws IOException {
        final TermsEnum termsEnum = leaf.terms(SYNTHETIC_ID).iterator();
        for (int i = 0; i < probes.size(); i++) {
            bh.consume(termsEnum.seekCeil(probes.get(i)));
        }
    }
}
