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
 * Bulk write benchmark for the SORTED doc values terms-dict path: indexes {@code docCount}
 * documents with a single {@link SortedDocValuesField} (a {@code _tsid} field shaped via
 * {@link TsidBuilder} from realistic TSDB dimensions) and commits once at the end. Runs
 * entirely in memory on a {@link ByteBuffersDirectory} with merging disabled, and configures
 * the index sort on {@code _tsid} so the codec exercises the SORTED terms-dict write path.
 *
 * <p>Only the {@code _tsid} field is indexed (not its dimensions) to keep per-doc allocation
 * noise low and make the terms-dict-encoder contribution easier to read in the profiler
 * output.
 *
 * <p>Purpose: allocation profiling of the terms-dict write path. Run with
 * {@code -prof gc.alloc.rate.norm} to confirm that the LZ4 hash table used by
 * {@code LZ4TermsDictEncoder} is allocated at most once per indexing thread thanks to
 * its {@link ThreadLocal} field, rather than once per field flush.
 *
 * <h2>Ready to run command</h2>
 *
 * <pre>{@code
 * ./gradlew :benchmarks:run --args="TSDBSortedDocValuesBulkWriteBenchmark \
 *   -f 3 -wi 3 -i 5 -prof gc"
 * }</pre>
 *
 * <p>The {@code LZ4.FastCompressionHashTable} used by the terms-dict encoder is now a
 * {@link ThreadLocal} field on {@code LZ4TermsDictEncoder}, so the
 * {@code gc.alloc.rate.norm} profile should show no per-op hash table allocations after
 * warmup.
 */
@Fork(1)
@Warmup(iterations = 2)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class TSDBSortedDocValuesBulkWriteBenchmark {

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

    @Param({ "100000" })
    private int docCount;

    private BytesRef[] tsidValues;
    private Codec es819Codec;

    private Directory directory;
    private IndexWriter writer;

    @Setup(Level.Trial)
    public void setupTrial() {
        final DocValuesFormat es819Format = ES819TSDBDocValuesFormatFactory.createDocValuesFormat(
            IndexVersion.current(),
            false,
            false,
            false,
            true
        );
        es819Codec = wrapCodec(es819Format);

        final IndexVersion indexVersion = IndexVersion.current();
        final Random random = new Random(42L);
        tsidValues = new BytesRef[docCount];
        for (int i = 0; i < docCount; i++) {
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
            tsidValues[i] = TsidBuilder.newBuilder()
                .addStringDimension("cluster.name", cluster)
                .addStringDimension("region", region)
                .addStringDimension("host.name", hostName)
                .addStringDimension("host.ip", hostIp)
                .addIntDimension("attributes.cpu", cpu)
                .buildTsid(indexVersion);
        }
    }

    @Setup(Level.Iteration)
    public void setupIteration() throws IOException {
        directory = new ByteBuffersDirectory();
        writer = newWriter(directory, es819Codec);
    }

    @TearDown(Level.Iteration)
    public void tearDownIteration() throws IOException {
        IOUtils.close(writer, directory);
        writer = null;
        directory = null;
    }

    /**
     * Writes every prepared document into a fresh {@link IndexWriter} and commits once at
     * the end, so allocation profilers see the SORTED terms-dict flush path for the
     * {@code _tsid} index-sort field.
     */
    @Benchmark
    public void bulkWriteSortedDocValues() throws IOException {
        writeAll(writer);
    }

    private void writeAll(final IndexWriter target) throws IOException {
        final Document doc = new Document();
        final SortedDocValuesField tsidField = new SortedDocValuesField(TSID_FIELD, new BytesRef());
        doc.add(tsidField);
        for (int i = 0; i < docCount; i++) {
            tsidField.setBytesValue(tsidValues[i]);
            target.addDocument(doc);
        }
        target.commit();
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
        config.setIndexSort(new Sort(new SortField(TSID_FIELD, SortField.Type.STRING)));
        return new IndexWriter(directory, config);
    }
}
