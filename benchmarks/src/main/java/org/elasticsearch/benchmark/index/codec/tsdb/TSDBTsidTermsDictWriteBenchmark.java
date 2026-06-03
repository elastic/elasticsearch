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
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.cluster.routing.TsidBuilder;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.codec.Elasticsearch93Lucene104Codec;
import org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormatFactory;
import org.elasticsearch.index.codec.tsdb.es95.ES95TSDBDocValuesFormatFactory;
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
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Bulk write benchmark for the {@code _tsid} terms-dictionary path. Indexes {@code docCount}
 * documents with a single {@link SortedDocValuesField} for {@code _tsid} (shaped via
 * {@link TsidBuilder} from realistic TSDB dimensions) and commits once at the end, with the
 * index sort set on {@code _tsid} so the SORTED terms-dict write path is exercised end to end.
 *
 * <p>Four codec variants are timed side by side via separate {@link Benchmark} methods:
 * <ul>
 *   <li>ES819 with LZ4-compressed {@code _tsid} terms dict (production today).</li>
 *   <li>ES819 with raw {@code _tsid} terms dict (FF on, snapshot today).</li>
 *   <li>ES95 with LZ4-compressed {@code _tsid} terms dict (synthetic baseline).</li>
 *   <li>ES95 with raw {@code _tsid} terms dict (target end state once ES95 ships).</li>
 * </ul>
 *
 * <p>Throughput only. Storage savings are measured in
 * {@code TsidTermsDictLz4SkipTests#testTermsDictBytesAcrossCodecVariants} via the producer's
 * {@code termsDataLength} (the precise terms-dict body byte count, isolated from the rest of the
 * {@code .dvd} file). Allocation pressure is captured by running this benchmark with
 * async-profiler in alloc mode.
 */
@Fork(1)
@Warmup(iterations = 2)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class TSDBTsidTermsDictWriteBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    private static final String TSID_FIELD = "_tsid";

    private static final String[] CLUSTER_NAMES = {
        "prod-eu-west",
        "prod-us-east",
        "prod-ap-southeast",
        "staging-eu-west",
        "staging-us-east" };

    private static final String[] REGIONS = { "eu-west-1", "us-east-1", "us-west-2", "ap-southeast-2", "eu-central-1" };

    @Param({ "10000", "100000", "1000000" })
    private int docCount;

    @Param({ "1000", "10000", "100000", "1000000" })
    private int cardinality;

    private BytesRef[] tsidValues;

    private Codec es819Lz4Codec;
    private Codec es819RawCodec;
    private Codec es95Lz4Codec;
    private Codec es95RawCodec;

    private Directory es819Lz4Directory;
    private IndexWriter es819Lz4Writer;
    private Directory es819RawDirectory;
    private IndexWriter es819RawWriter;
    private Directory es95Lz4Directory;
    private IndexWriter es95Lz4Writer;
    private Directory es95RawDirectory;
    private IndexWriter es95RawWriter;

    @Setup(Level.Trial)
    public void setupTrial() {
        es819Lz4Codec = wrapCodec(ES819TSDBDocValuesFormatFactory.createDocValuesFormat(IndexVersion.current(), false, false, true, false));
        es819RawCodec = wrapCodec(ES819TSDBDocValuesFormatFactory.createDocValuesFormat(IndexVersion.current(), false, false, true, true));
        es95Lz4Codec = wrapCodec(ES95TSDBDocValuesFormatFactory.get(false, false, true, false));
        es95RawCodec = wrapCodec(ES95TSDBDocValuesFormatFactory.get(false, false, true, true));

        final int effectiveCardinality = Math.min(cardinality, docCount);
        final BytesRef[] pool = generateUniqueTsidPool(effectiveCardinality);
        tsidValues = new BytesRef[docCount];
        for (int i = 0; i < docCount; i++) {
            tsidValues[i] = pool[i % effectiveCardinality];
        }
    }

    private static BytesRef[] generateUniqueTsidPool(final int n) {
        final IndexVersion indexVersion = IndexVersion.current();
        final Random random = new Random(42L);
        final java.util.HashSet<BytesRef> seen = new java.util.HashSet<>(n * 2);
        final BytesRef[] pool = new BytesRef[n];
        int filled = 0;
        while (filled < n) {
            final String cluster = CLUSTER_NAMES[random.nextInt(CLUSTER_NAMES.length)];
            final String region = REGIONS[random.nextInt(REGIONS.length)];
            final int hostId = random.nextInt(10_000);
            final String hostName = "host-" + Integer.toHexString(hostId);
            final String hostIp = (10 + (hostId >>> 16) % 245)
                + "."
                + ((hostId >>> 8) & 0xFF)
                + "."
                + (hostId & 0xFF)
                + "."
                + random.nextInt(255);
            final int cpu = random.nextInt(16);
            final BytesRef tsid = TsidBuilder.newBuilder()
                .addStringDimension("cluster.name", cluster)
                .addStringDimension("region", region)
                .addStringDimension("host.name", hostName)
                .addStringDimension("host.ip", hostIp)
                .addIntDimension("attributes.cpu", cpu)
                .buildTsid(indexVersion);
            if (seen.add(tsid)) {
                pool[filled++] = tsid;
            }
        }
        return pool;
    }

    @Setup(Level.Iteration)
    public void setupIteration() throws IOException {
        es819Lz4Directory = new ByteBuffersDirectory();
        es819Lz4Writer = newWriter(es819Lz4Directory, es819Lz4Codec);
        es819RawDirectory = new ByteBuffersDirectory();
        es819RawWriter = newWriter(es819RawDirectory, es819RawCodec);
        es95Lz4Directory = new ByteBuffersDirectory();
        es95Lz4Writer = newWriter(es95Lz4Directory, es95Lz4Codec);
        es95RawDirectory = new ByteBuffersDirectory();
        es95RawWriter = newWriter(es95RawDirectory, es95RawCodec);
    }

    @TearDown(Level.Iteration)
    public void tearDownIteration() throws IOException {
        IOUtils.close(
            es819Lz4Writer,
            es819Lz4Directory,
            es819RawWriter,
            es819RawDirectory,
            es95Lz4Writer,
            es95Lz4Directory,
            es95RawWriter,
            es95RawDirectory
        );
        es819Lz4Writer = null;
        es819Lz4Directory = null;
        es819RawWriter = null;
        es819RawDirectory = null;
        es95Lz4Writer = null;
        es95Lz4Directory = null;
        es95RawWriter = null;
        es95RawDirectory = null;
    }

    @Benchmark
    public void bulkWriteES819Lz4() throws IOException {
        writeAll(es819Lz4Writer);
    }

    @Benchmark
    public void bulkWriteES819Raw() throws IOException {
        writeAll(es819RawWriter);
    }

    @Benchmark
    public void bulkWriteES95Lz4() throws IOException {
        writeAll(es95Lz4Writer);
    }

    @Benchmark
    public void bulkWriteES95Raw() throws IOException {
        writeAll(es95RawWriter);
    }

    private void writeAll(final IndexWriter writer) throws IOException {
        final Document doc = new Document();
        final SortedDocValuesField tsidField = new SortedDocValuesField(TSID_FIELD, new BytesRef());
        doc.add(tsidField);
        for (int i = 0; i < docCount; i++) {
            tsidField.setBytesValue(tsidValues[i]);
            writer.addDocument(doc);
        }
        writer.commit();
    }

    private static Codec wrapCodec(final DocValuesFormat dvFormat) {
        return new Elasticsearch93Lucene104Codec() {
            @Override
            public DocValuesFormat getDocValuesFormatForField(final String field) {
                return dvFormat;
            }
        };
    }

    private static IndexWriter newWriter(final Directory directory, final Codec codec) throws IOException {
        final IndexWriterConfig config = new IndexWriterConfig();
        config.setCodec(codec);
        config.setMergePolicy(NoMergePolicy.INSTANCE);
        config.setIndexSort(new Sort(new SortField(TSID_FIELD, SortField.Type.STRING)));
        return new IndexWriter(directory, config);
    }
}
