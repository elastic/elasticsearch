/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.benchmark._nightly.esql;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.xpack.esql.datasource.orc.OrcFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
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
import org.openjdk.jmh.annotations.Warmup;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Measures end-to-end {@link OrcFormatReader#read} throughput over an in-memory ORC
 * fixture (built once to a temp file, then loaded back into a byte array). Targets
 * the ORC vectorized row-batch decode path. Variants cover column projection (all
 * four columns vs a two-column subset). Fixture compression is
 * {@link CompressionKind#NONE} to keep codec init out of the measured signal.
 */
@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public class OrcReadBenchmark {

    static {
        if (false == "true".equals(System.getProperty("skipSelfTest"))) {
            selfTest();
        }
    }

    @Param({ "10000", "100000" })
    public int rowCount;

    @Param({ "all", "projectedSubset" })
    public String projection;

    private BlockFactory blockFactory;
    private StorageObject storageObject;
    private long fixtureBytes;
    private List<String> projectedColumns;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        Utils.configureBenchmarkLogging();
        blockFactory = DatasourceBenchmarks.newBlockFactory();
        byte[] orcBytes = generateOrcFixture(rowCount);
        fixtureBytes = orcBytes.length;
        storageObject = DatasourceBenchmarks.inMemoryStorageObject(orcBytes, "memory://bench.orc");
        projectedColumns = switch (projection) {
            case "all" -> List.of("id", "value", "name", "score");
            case "projectedSubset" -> List.of("id", "name");
            default -> throw new IllegalArgumentException("unknown projection: " + projection);
        };
    }

    @Benchmark
    public int readAll(ReadMetrics metrics) throws IOException {
        OrcFormatReader reader = new OrcFormatReader(blockFactory);
        FormatReadContext ctx = FormatReadContext.builder().projectedColumns(projectedColumns).batchSize(1000).build();
        int totalRows = 0;
        try (CloseableIterator<Page> iter = reader.read(storageObject, ctx)) {
            while (iter.hasNext()) {
                Page page = iter.next();
                totalRows += page.getPositionCount();
                page.releaseBlocks();
            }
        }
        metrics.record(totalRows, fixtureBytes);
        return totalRows;
    }

    static void selfTest() {
        for (String projection : Utils.possibleValues(OrcReadBenchmark.class, "projection")) {
            OrcReadBenchmark bench = new OrcReadBenchmark();
            bench.rowCount = DatasourceBenchmarks.SELF_TEST_ROW_COUNT;
            bench.projection = projection;
            try {
                bench.setup();
                int actual = bench.readAll(new ReadMetrics());
                if (actual != bench.rowCount) {
                    throw new AssertionError("OrcReadBenchmark[" + projection + "] read " + actual + " rows, expected " + bench.rowCount);
                }
            } catch (IOException e) {
                throw new AssertionError("OrcReadBenchmark[" + projection + "] failed", e);
            }
        }
    }

    private static TypeDescription schema() {
        return TypeDescription.createStruct()
            .addField("id", TypeDescription.createLong())
            .addField("value", TypeDescription.createInt())
            .addField("name", TypeDescription.createString())
            .addField("score", TypeDescription.createDouble());
    }

    /**
     * Builds an ORC file on-disk (ORC's writer requires a Hadoop {@code Path}), reads its
     * bytes back, and deletes the temp file. The benchmark reader then operates entirely
     * in-memory via {@link DatasourceBenchmarks#inMemoryStorageObject}.
     * <p>
     * The fixture lives inside a freshly-created temp directory so concurrent Gradle
     * test workers can't race on the file name — the directory's unique name isolates
     * each call.
     */
    static byte[] generateOrcFixture(int rowCount) throws IOException {
        var tempDir = Files.createTempDirectory("orc-bench");
        var tempPath = tempDir.resolve("bench.orc");
        try {
            Path orcPath = new Path(tempPath.toUri());
            Configuration conf = new Configuration(false);
            conf.set("orc.key.provider", "memory");
            NoPermissionLocalFileSystem localFs = new NoPermissionLocalFileSystem();
            localFs.setConf(conf);
            TypeDescription schema = schema();
            OrcFile.WriterOptions writerOptions = OrcFile.writerOptions(conf)
                .setSchema(schema)
                .fileSystem(localFs)
                .compress(CompressionKind.NONE);

            try (Writer writer = OrcFile.createWriter(orcPath, writerOptions)) {
                VectorizedRowBatch batch = schema.createRowBatch();
                LongColumnVector idCol = (LongColumnVector) batch.cols[0];
                LongColumnVector valueCol = (LongColumnVector) batch.cols[1];
                BytesColumnVector nameCol = (BytesColumnVector) batch.cols[2];
                DoubleColumnVector scoreCol = (DoubleColumnVector) batch.cols[3];
                for (int i = 0; i < rowCount; i++) {
                    int row = batch.size++;
                    idCol.vector[row] = i;
                    valueCol.vector[row] = i;
                    nameCol.setVal(row, ("row-" + i).getBytes(StandardCharsets.UTF_8));
                    scoreCol.vector[row] = i * 1.5;
                    if (batch.size == batch.getMaxSize()) {
                        writer.addRowBatch(batch);
                        batch.reset();
                    }
                }
                if (batch.size > 0) {
                    writer.addRowBatch(batch);
                }
            }
            return Files.readAllBytes(tempPath);
        } finally {
            Files.deleteIfExists(tempPath);
            Files.deleteIfExists(tempDir);
        }
    }

    /**
     * Bypass for {@code Shell.<clinit>} — the same workaround used by the ORC reader's tests
     * (see {@code OrcFormatReaderTests.NoPermissionLocalFileSystem}). Hadoop's default
     * {@code LocalFSFileOutputStream} static-inits {@code Shell}, which only works on
     * platforms with {@code chmod} available; in a benchmark JVM we avoid that path entirely
     * by writing through {@link FileOutputStream} and no-op'ing permission/mkdir calls.
     */
    private static final class NoPermissionLocalFileSystem extends RawLocalFileSystem {
        @Override
        @SuppressForbidden(reason = "Bypass Hadoop's LocalFSFileOutputStream to avoid Shell.<clinit>")
        protected OutputStream createOutputStreamWithMode(Path p, boolean append, FsPermission permission) throws IOException {
            return new FileOutputStream(pathToFile(p), append);
        }

        @Override
        public void setPermission(Path p, FsPermission permission) {
            // no-op: skip chmod calls that would trigger Shell
        }

        @Override
        @SuppressForbidden(reason = "Hadoop API requires java.io.File in method signature")
        protected boolean mkOneDirWithMode(Path p, File p2f, FsPermission permission) throws IOException {
            return p2f.mkdir() || p2f.isDirectory();
        }
    }
}
