/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.tsdb;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.codec.Elasticsearch93Lucene104Codec;
import org.elasticsearch.index.codec.tsdb.es95.ES95TSDBDocValuesFormatFactory;
import org.openjdk.jmh.annotations.AuxCounters;
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
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * Compares the disk footprint of the ES95 TSDB doc values format with and without
 * adaptive ordinal blocks. Indexes a fixed number of documents that share a
 * keyword dimension across consecutive blocks (mimicking the
 * tsid-sorted-runs shape that adaptive CONST and RLE modes target) and prints
 * the total directory size for each codec on tear down.
 *
 * <p>Indexing happens entirely in memory on a {@code ByteBuffersDirectory} with
 * merging disabled so the measurement isolates per-segment doc values bytes
 * from disk IO and merge cost. The {@code @Benchmark} time measurement is a
 * by-product; the headline result is the {@code [storage]} line in stdout.
 *
 * <h2>Ready to run command</h2>
 *
 * <pre>{@code
 * ./gradlew :benchmarks:run --args="AdaptiveOrdinalCodecBenchmark \
 *   -wi 5 -i 5 -f 3"
 * }</pre>
 */
@Fork(1)
@Warmup(iterations = 2)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class AdaptiveOrdinalCodecBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    private static final String SORTED_FIELD = "host_name";
    private static final String SORTED_SET_FIELD = "host_ip";

    @Param({ "100000" })
    private int docCount;

    // NOTE: documents are grouped into runs of `docsPerHost` consecutive docs that share the
    // same host name and host ip set, which is the shape produced by TSDB index sort on _tsid.
    // Higher values stretch CONST mode runs, lower values force RLE / BITPACK_LOCAL.
    @Param({ "128", "1024" })
    private int docsPerHost;

    private String[] hostNames;
    private BytesRef[][] hostIps;

    private Codec legacyCodec;
    private Codec adaptiveCodec;

    private Directory legacyDirectory;
    private IndexWriter legacyWriter;
    private Directory adaptiveDirectory;
    private IndexWriter adaptiveWriter;

    @Setup(Level.Trial)
    public void setupTrial() {
        final DocValuesFormat legacyFormat = ES95TSDBDocValuesFormatFactory.get(false, false, false, false);
        final DocValuesFormat adaptiveFormat = ES95TSDBDocValuesFormatFactory.get(false, false, false, true);
        legacyCodec = wrapCodec(legacyFormat);
        adaptiveCodec = wrapCodec(adaptiveFormat);

        final int hostCount = Math.max(1, docCount / docsPerHost);
        hostNames = new String[hostCount];
        hostIps = new BytesRef[hostCount][];
        for (int i = 0; i < hostCount; i++) {
            hostNames[i] = String.format(java.util.Locale.ROOT, "host-%05d.example.com", i);
            // NOTE: three IPs per host modeled after a typical hostmetrics doc: loopback,
            // a per-host private IP, and a per-host docker bridge IP. Adaptive RLE wins big
            // when every doc in a run repeats the same multi-value set.
            hostIps[i] = new BytesRef[] {
                new BytesRef("127.0.0.1".getBytes(StandardCharsets.US_ASCII)),
                new BytesRef(
                    String.format(java.util.Locale.ROOT, "10.0.%d.%d", (i >>> 8) & 0xff, i & 0xff).getBytes(StandardCharsets.US_ASCII)
                ),
                new BytesRef(
                    String.format(java.util.Locale.ROOT, "172.17.%d.%d", (i >>> 8) & 0xff, i & 0xff).getBytes(StandardCharsets.US_ASCII)
                ) };
        }
    }

    @Setup(Level.Iteration)
    public void setupIteration() throws IOException {
        legacyDirectory = new ByteBuffersDirectory();
        legacyWriter = newWriter(legacyDirectory, legacyCodec);
        adaptiveDirectory = new ByteBuffersDirectory();
        adaptiveWriter = newWriter(adaptiveDirectory, adaptiveCodec);
    }

    @TearDown(Level.Iteration)
    public void tearDownIteration() throws IOException {
        final long legacyBytes = directorySize(legacyDirectory);
        final long adaptiveBytes = directorySize(adaptiveDirectory);
        final double ratio = legacyBytes == 0 ? Double.NaN : ((double) adaptiveBytes) / legacyBytes;
        System.out.printf(
            java.util.Locale.ROOT,
            "[storage] docCount=%d docsPerHost=%d legacyBytes=%d adaptiveBytes=%d adaptive/legacy=%.4f%n",
            docCount,
            docsPerHost,
            legacyBytes,
            adaptiveBytes,
            ratio
        );
        IOUtils.close(legacyWriter, legacyDirectory, adaptiveWriter, adaptiveDirectory);
        legacyWriter = null;
        legacyDirectory = null;
        adaptiveWriter = null;
        adaptiveDirectory = null;
    }

    /**
     * Per-benchmark storage counters reported in the JMH JSON output. Each benchmark method
     * populates {@link #directoryBytes} with the resulting in-memory directory size; the
     * value lands in JMH's per-iteration table next to the timing score so the JSON
     * artifact alone is enough to extract the storage comparison.
     */
    @State(Scope.Thread)
    @AuxCounters(AuxCounters.Type.EVENTS)
    public static class StorageCounters {
        public long directoryBytes;
    }

    @Benchmark
    public void bulkWriteES95Legacy(StorageCounters counters) throws IOException {
        writeAll(legacyWriter);
        counters.directoryBytes = directorySize(legacyDirectory);
    }

    @Benchmark
    public void bulkWriteES95Adaptive(StorageCounters counters) throws IOException {
        writeAll(adaptiveWriter);
        counters.directoryBytes = directorySize(adaptiveDirectory);
    }

    private void writeAll(final IndexWriter writer) throws IOException {
        final SortedDocValuesField sortedField = new SortedDocValuesField(SORTED_FIELD, new BytesRef());
        for (int i = 0; i < docCount; i++) {
            final int hostIndex = i / docsPerHost % hostNames.length;
            final Document doc = new Document();
            sortedField.setBytesValue(new BytesRef(hostNames[hostIndex].getBytes(StandardCharsets.US_ASCII)));
            doc.add(sortedField);
            for (BytesRef ip : hostIps[hostIndex]) {
                doc.add(new SortedSetDocValuesField(SORTED_SET_FIELD, ip));
            }
            writer.addDocument(doc);
        }
        writer.commit();
    }

    private static long directorySize(final Directory directory) throws IOException {
        long total = 0L;
        for (String name : directory.listAll()) {
            total += directory.fileLength(name);
        }
        return total;
    }

    private static Codec wrapCodec(final DocValuesFormat dvFormat) {
        return new Elasticsearch93Lucene104Codec() {
            @Override
            public DocValuesFormat getDocValuesFormatForField(String field) {
                return dvFormat;
            }
        };
    }

    private static IndexWriter newWriter(final Directory directory, final Codec codec) throws IOException {
        final IndexWriterConfig config = new IndexWriterConfig();
        config.setCodec(codec);
        config.setMergePolicy(NoMergePolicy.INSTANCE);
        return new IndexWriter(directory, config);
    }
}
