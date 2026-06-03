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
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.TermsEnum;
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
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Bulk read benchmark for the {@code _tsid} terms-dictionary path. Builds four pre-populated
 * indexes once in trial setup (one per codec variant) and then walks each one's
 * {@code _tsid} {@link TermsEnum} in a separate {@link Benchmark} method, exercising the per
 * block decoder for every block in the dictionary.
 *
 * <p>{@link TermsEnum#next()} walks the dictionary block by block, so this measures both the
 * block decode cost (LZ4 decompress vs raw copy) and the surrounding prefix-suffix term
 * reconstruction. Production paths that hit this code include {@code _tsid} terms aggregations
 * and time-series searcher iteration.
 *
 * <p>Four codec variants are timed side by side:
 * <ul>
 *   <li>ES819 with LZ4-compressed {@code _tsid} terms dict (production today).</li>
 *   <li>ES819 with raw {@code _tsid} terms dict (FF on, snapshot today).</li>
 *   <li>ES95 with LZ4-compressed {@code _tsid} terms dict (synthetic baseline).</li>
 *   <li>ES95 with raw {@code _tsid} terms dict (target end state once ES95 ships).</li>
 * </ul>
 *
 * <h2>Ready to run commands</h2>
 *
 * <pre>{@code
 * # Throughput sweep:
 * ./gradlew :benchmarks:run --args="TSDBTsidTermsDictReadBenchmark -f 1 -wi 3 -i 5"
 *
 * # Allocation profile via async-profiler:
 * ./gradlew :benchmarks:run --args="TSDBTsidTermsDictReadBenchmark -f 1 -wi 3 -i 5 \
 *     -prof async:libPath=$HOME/async-profiler-4.2.1-macos/lib/libasyncProfiler.dylib;event=alloc;output=flamegraph"
 * }</pre>
 */
@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class TSDBTsidTermsDictReadBenchmark {

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

    private Directory es819Lz4Directory;
    private DirectoryReader es819Lz4Reader;
    private Directory es819RawDirectory;
    private DirectoryReader es819RawReader;
    private Directory es95Lz4Directory;
    private DirectoryReader es95Lz4Reader;
    private Directory es95RawDirectory;
    private DirectoryReader es95RawReader;

    @Setup(Level.Trial)
    public void setupTrial() throws IOException {
        final int effectiveCardinality = Math.min(cardinality, docCount);
        final BytesRef[] pool = generateUniqueTsidPool(effectiveCardinality);
        final BytesRef[] tsidValues = new BytesRef[docCount];
        for (int i = 0; i < docCount; i++) {
            tsidValues[i] = pool[i % effectiveCardinality];
        }

        final Codec es819Lz4Codec = wrapCodec(
            ES819TSDBDocValuesFormatFactory.createDocValuesFormat(IndexVersion.current(), false, false, true, false)
        );
        final Codec es819RawCodec = wrapCodec(
            ES819TSDBDocValuesFormatFactory.createDocValuesFormat(IndexVersion.current(), false, false, true, true)
        );
        final Codec es95Lz4Codec = wrapCodec(ES95TSDBDocValuesFormatFactory.get(false, false, true, false));
        final Codec es95RawCodec = wrapCodec(ES95TSDBDocValuesFormatFactory.get(false, false, true, true));

        es819Lz4Directory = new ByteBuffersDirectory();
        es819Lz4Reader = writeAndOpen(es819Lz4Directory, es819Lz4Codec, tsidValues);
        es819RawDirectory = new ByteBuffersDirectory();
        es819RawReader = writeAndOpen(es819RawDirectory, es819RawCodec, tsidValues);
        es95Lz4Directory = new ByteBuffersDirectory();
        es95Lz4Reader = writeAndOpen(es95Lz4Directory, es95Lz4Codec, tsidValues);
        es95RawDirectory = new ByteBuffersDirectory();
        es95RawReader = writeAndOpen(es95RawDirectory, es95RawCodec, tsidValues);
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() throws IOException {
        IOUtils.close(
            es819Lz4Reader,
            es819Lz4Directory,
            es819RawReader,
            es819RawDirectory,
            es95Lz4Reader,
            es95Lz4Directory,
            es95RawReader,
            es95RawDirectory
        );
        es819Lz4Reader = null;
        es819Lz4Directory = null;
        es819RawReader = null;
        es819RawDirectory = null;
        es95Lz4Reader = null;
        es95Lz4Directory = null;
        es95RawReader = null;
        es95RawDirectory = null;
    }

    @Benchmark
    public void iterateTermsEnumES819Lz4(final Blackhole bh) throws IOException {
        iterateAll(es819Lz4Reader, bh);
    }

    @Benchmark
    public void iterateTermsEnumES819Raw(final Blackhole bh) throws IOException {
        iterateAll(es819RawReader, bh);
    }

    @Benchmark
    public void iterateTermsEnumES95Lz4(final Blackhole bh) throws IOException {
        iterateAll(es95Lz4Reader, bh);
    }

    @Benchmark
    public void iterateTermsEnumES95Raw(final Blackhole bh) throws IOException {
        iterateAll(es95RawReader, bh);
    }

    private static void iterateAll(final DirectoryReader reader, final Blackhole bh) throws IOException {
        for (LeafReaderContext leaf : reader.leaves()) {
            final SortedDocValues sdv = leaf.reader().getSortedDocValues(TSID_FIELD);
            if (sdv == null) {
                continue;
            }
            final TermsEnum termsEnum = sdv.termsEnum();
            for (BytesRef term = termsEnum.next(); term != null; term = termsEnum.next()) {
                bh.consume(term);
            }
        }
    }

    private static DirectoryReader writeAndOpen(final Directory directory, final Codec codec, final BytesRef[] tsidValues)
        throws IOException {
        try (IndexWriter writer = newWriter(directory, codec)) {
            final Document doc = new Document();
            final SortedDocValuesField tsidField = new SortedDocValuesField(TSID_FIELD, new BytesRef());
            doc.add(tsidField);
            for (BytesRef tsidValue : tsidValues) {
                tsidField.setBytesValue(tsidValue);
                writer.addDocument(doc);
            }
            writer.commit();
        }
        return DirectoryReader.open(directory);
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
        config.setIndexSort(new Sort(new SortField(TSID_FIELD, SortField.Type.STRING)));
        return new IndexWriter(directory, config);
    }
}
