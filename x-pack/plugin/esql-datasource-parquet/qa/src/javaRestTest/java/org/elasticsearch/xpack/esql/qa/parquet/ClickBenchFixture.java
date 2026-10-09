/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.parquet;

import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.MessageType;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.datasources.FixtureUtils;
import org.elasticsearch.xpack.esql.datasources.HttpDownloadRetry;
import org.junit.rules.ExternalResource;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Test fixture that downloads the first row group (RG0) from a selection of ClickHouse ClickBench
 * partitioned Parquet files via HTTP range requests, then writes two local datasets under a
 * dedicated {@code clickbench-fixtures} directory (sibling of {@code iceberg-fixtures}):
 * <ul>
 *   <li>{@code clickbench/hits.parquet} — single file with all row groups merged</li>
 *   <li>{@code clickbench_multi/hits_1.parquet} through {@code hits_5.parquet} — five splits</li>
 * </ul>
 * The data lives outside {@code iceberg-fixtures} so that
 * {@link org.elasticsearch.xpack.esql.qa.rest.AbstractExternalSourceSpecTestCase#loadExternalSourceFixtures()}
 * does not load these large files into memory.
 * <p>
 * Tests use {@code file://} URIs to query these local datasets via ES|QL EXTERNAL.
 * <p>
 * The fixture selects the 18 partitioned files whose RG0 is smallest (~94 MB total download,
 * ~1.3M rows). Only RG0 bytes are downloaded via HTTP range requests — not the full files.
 * Row groups are copied verbatim (no decode/encode) using {@link ParquetFileWriter#appendRowGroups}.
 * <p>
 * Intermediate single-RG0 files are written to disk (not kept in memory) to avoid OOM with large
 * file padding. Downloaded RG0 files are written to a temporary path and atomically renamed on
 * completion, so a partial download from a previous interrupted run is never treated as cached.
 */
public class ClickBenchFixture extends ExternalResource {

    private static final Logger logger = LogManager.getLogger(ClickBenchFixture.class);

    private static final String BASE_URL = "https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_";
    private static final String SUFFIX = ".parquet";
    private static final int REACHABILITY_TIMEOUT_MS = 10_000;
    private static final int DOWNLOAD_TIMEOUT_MS = 120_000;
    private static final byte[] PARQUET_MAGIC = { 'P', 'A', 'R', '1' };
    private static final int SPLIT_COUNT = 5;

    // Same retry policy as ParquetTestingIT#downloadFile, so the two fixtures behave consistently
    // against occasional rate-limiting/transient errors from their respective third-party hosts.
    private static final int DOWNLOAD_MAX_ATTEMPTS = 4;
    private static final long DOWNLOAD_INITIAL_BACKOFF_MILLIS = 1000L;
    private static final long DOWNLOAD_MAX_BACKOFF_MILLIS = 8000L;

    /**
     * The 18 file indices with the smallest RG0, sorted by RG0 compressed size ascending.
     * Determined by probing all 100 partitioned files' Parquet footers.
     * Total RG0 download: ~93 MB, ~1.28M rows.
     */
    static final int[] SOURCE_FILE_INDICES = { 31, 2, 22, 49, 35, 62, 20, 78, 67, 97, 10, 36, 32, 48, 1, 75, 53, 72 };

    private static volatile Boolean reachable;

    private Path fixturesRoot;

    /** Whether the remote ClickBench data is reachable. Safe to call before/after the rule runs. */
    public static boolean isDataReachable() {
        if (reachable == null) {
            synchronized (ClickBenchFixture.class) {
                if (reachable == null) {
                    reachable = checkReachability();
                }
            }
        }
        return reachable;
    }

    /**
     * Path to the iceberg-fixtures root directory where generated files live.
     * Returns null if the fixture was not set up (e.g. data unreachable).
     */
    public Path fixturesRoot() {
        return fixturesRoot;
    }

    @Override
    @SuppressForbidden(reason = "Writing intermediate Parquet files to build output directory")
    protected void before() throws Throwable {
        if (isDataReachable() == false) {
            logger.info("ClickBench data not reachable, skipping fixture setup");
            return;
        }

        Path icebergFixtures = FixtureUtils.resolveLocalFixturesPath(logger, ClickBenchFixture.class);
        if (icebergFixtures == null) {
            logger.warn("Cannot resolve iceberg-fixtures path, ClickBench tests will be skipped");
            return;
        }

        // Write clickbench data to a sibling directory so the large files are not picked up by
        // loadExternalSourceFixtures(), which walks iceberg-fixtures/ and reads every file into memory.
        Path clickbenchRoot = icebergFixtures.getParent().resolve("clickbench-fixtures");
        Path singleDir = clickbenchRoot.resolve("clickbench");
        Path multiDir = clickbenchRoot.resolve("clickbench_multi");
        Path rawDir = clickbenchRoot.resolve("clickbench_raw");
        Files.createDirectories(singleDir);
        Files.createDirectories(multiDir);
        Files.createDirectories(rawDir);

        Path singleFile = singleDir.resolve("hits.parquet");

        boolean allExist = Files.exists(singleFile);
        for (int i = 1; i <= SPLIT_COUNT && allExist; i++) {
            allExist = Files.exists(multiDir.resolve("hits_" + i + ".parquet"));
        }
        if (allExist) {
            logger.info("ClickBench fixtures already exist at [{}], skipping download", clickbenchRoot);
            fixturesRoot = clickbenchRoot;
            return;
        }

        logger.info("Downloading ClickBench RG0 from {} source files", SOURCE_FILE_INDICES.length);
        List<Path> rg0Paths = new ArrayList<>();

        for (int idx : SOURCE_FILE_INDICES) {
            Path rg0File = rawDir.resolve("rg0_" + idx + ".parquet");
            if (Files.exists(rg0File) && Files.size(rg0File) > 0) {
                logger.info("RG0 for hits_{} already cached at [{}]", idx, rg0File);
                rg0Paths.add(rg0File);
                continue;
            }

            String url = BASE_URL + idx + SUFFIX;
            logger.info("Downloading RG0 from hits_{}", idx);
            Path tmpFile = rawDir.resolve("rg0_" + idx + ".parquet.tmp");
            boolean ok;
            try {
                ok = HttpDownloadRetry.withRetries(
                    logger,
                    "download RG0 from hits_" + idx,
                    DOWNLOAD_MAX_ATTEMPTS,
                    DOWNLOAD_INITIAL_BACKOFF_MILLIS,
                    DOWNLOAD_MAX_BACKOFF_MILLIS,
                    () -> downloadRG0ToFile(url, tmpFile)
                );
            } catch (IOException e) {
                // A single source file's download failure that persists across retries (e.g. a permanent
                // 404, or transient errors that outlasted the retry budget) should not abort the whole
                // fixture and fail every test in the suite -- skip this file like the ok == false path.
                logger.warn("Failed to download RG0 from hits_{}, skipping: {}", idx, e.getMessage());
                Files.deleteIfExists(tmpFile);
                continue;
            }
            if (ok == false) {
                logger.warn("Failed to download RG0 from hits_{}, skipping", idx);
                Files.deleteIfExists(tmpFile);
                continue;
            }
            Files.move(tmpFile, rg0File, StandardCopyOption.REPLACE_EXISTING);
            rg0Paths.add(rg0File);
            logger.info("Downloaded RG0 #{} ({} bytes), {} files so far", idx, Files.size(rg0File), rg0Paths.size());
        }

        if (rg0Paths.isEmpty()) {
            logger.warn("No RG0 files downloaded, fixture will be empty");
            fixturesRoot = clickbenchRoot;
            return;
        }

        MessageType schema;
        try (ParquetFileReader reader = openReader(new FileInputFile(rg0Paths.get(0)))) {
            schema = reader.getFooter().getFileMetaData().getSchema();
        }

        logger.info("Writing single-file dataset from {} source RG0s", rg0Paths.size());
        mergeRowGroups(singleFile, schema, rg0Paths, 0, rg0Paths.size());

        int totalFiles = rg0Paths.size();
        logger.info("Writing {}-file split dataset", SPLIT_COUNT);
        for (int i = 0; i < SPLIT_COUNT; i++) {
            int from = totalFiles * i / SPLIT_COUNT;
            int to = totalFiles * (i + 1) / SPLIT_COUNT;
            Path splitFile = multiDir.resolve("hits_" + (i + 1) + ".parquet");
            mergeRowGroups(splitFile, schema, rg0Paths, from, to);
        }

        fixturesRoot = clickbenchRoot;
        logger.info("ClickBench fixture ready at [{}]", fixturesRoot);
    }

    @Override
    protected void after() {
        // Files persist in the build output directory across runs; cleaned by ./gradlew clean.
    }

    /**
     * Open a {@link ParquetFileReader} using {@link PlainParquetConfiguration} to avoid
     * requiring Hadoop runtime classes on the classpath.
     */
    private static ParquetFileReader openReader(InputFile inputFile) throws IOException {
        ParquetReadOptions options = ParquetReadOptions.builder(new PlainParquetConfiguration()).build();
        return new ParquetFileReader(inputFile, options);
    }

    /**
     * Merge row groups from a subset of on-disk single-RG0 Parquet files into a single output file.
     * Uses {@link ParquetFileWriter#appendRowGroups(SeekableInputStream, List, boolean)} for
     * zero-copy row group transfer — column data is copied byte-for-byte without decode/encode.
     * Writes directly to disk via a file-backed {@link OutputFile} to avoid holding the entire
     * merged output in memory.
     */
    @SuppressForbidden(reason = "Writing Parquet files to build output directory")
    private static void mergeRowGroups(Path outputPath, MessageType schema, List<Path> sources, int from, int to) throws IOException {
        OutputFile outputFile = createFileOutputFile(outputPath);
        try (ParquetFileWriter writer = new ParquetFileWriter(outputFile, schema, ParquetFileWriter.Mode.CREATE, 128 * 1024 * 1024, 0)) {
            writer.start();
            for (int i = from; i < to; i++) {
                FileInputFile inputFile = new FileInputFile(sources.get(i));
                try (ParquetFileReader reader = openReader(inputFile)) {
                    List<BlockMetaData> blocks = reader.getFooter().getBlocks();
                    // Only copy RG0 — the intermediate files contain only the first row group's data,
                    // even though the original footer references all row groups from the source file.
                    try (SeekableInputStream stream = inputFile.newStream()) {
                        writer.appendRowGroups(stream, blocks.subList(0, 1), false);
                    }
                }
            }
            writer.end(Map.of());
        }
    }

    @SuppressForbidden(reason = "HTTP HEAD to check remote data availability")
    private static boolean checkReachability() {
        try {
            URI uri = URI.create(BASE_URL + SOURCE_FILE_INDICES[0] + SUFFIX);
            HttpURLConnection conn = (HttpURLConnection) uri.toURL().openConnection();
            conn.setRequestMethod("HEAD");
            conn.setConnectTimeout(REACHABILITY_TIMEOUT_MS);
            conn.setReadTimeout(REACHABILITY_TIMEOUT_MS);
            int code = conn.getResponseCode();
            conn.disconnect();
            boolean ok = code == 200;
            logger.info("ClickBench reachability check: {} (HTTP {})", ok ? "reachable" : "unreachable", code);
            return ok;
        } catch (Exception e) {
            logger.info("ClickBench reachability check failed: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Downloads only the first row group from a remote Parquet file using HTTP range requests,
     * then writes a valid single-RG0 Parquet file to disk. The file preserves original byte
     * offsets from the footer (padded with zeros) so row group metadata remains valid.
     */
    @SuppressForbidden(reason = "HTTP range requests and disk I/O for partial Parquet files")
    private static boolean downloadRG0ToFile(String fileUrl, Path outputPath) throws IOException {
        long fileSize = httpFileSize(fileUrl);
        if (fileSize <= 0) {
            return false;
        }

        byte[] tail = httpRange(fileUrl, fileSize - 8, fileSize - 1);
        int footerLen = ByteBuffer.wrap(tail, 0, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();

        long footerStart = fileSize - footerLen - 8;
        byte[] footer = httpRange(fileUrl, footerStart, fileSize - 1);

        byte[] minimalParquet = buildMinimalParquet(footer, footerLen);

        long rg0Start;
        long rg0End;
        try (ParquetFileReader metaReader = openReader(new ByteArrayInputFile(minimalParquet))) {
            var blocks = metaReader.getFooter().getBlocks();
            if (blocks.isEmpty()) {
                return false;
            }
            var rg0 = blocks.get(0);
            rg0Start = Long.MAX_VALUE;
            rg0End = 0;
            for (var col : rg0.getColumns()) {
                long colStart = col.getStartingPos();
                long colEnd = colStart + col.getTotalSize();
                rg0Start = Math.min(rg0Start, colStart);
                rg0End = Math.max(rg0End, colEnd);
            }
        }

        byte[] rg0Data = httpRange(fileUrl, rg0Start, rg0End - 1);

        // Write assembled Parquet file to disk, preserving original absolute offsets.
        // Padding is written directly to file, not held in memory.
        try (RandomAccessFile raf = new RandomAccessFile(outputPath.toFile(), "rw")) {
            raf.write(PARQUET_MAGIC);
            long padding = rg0Start - 4;
            if (padding > 0) {
                raf.seek(rg0Start);
            }
            raf.write(rg0Data);
            long currentPos = rg0Start + rg0Data.length;
            if (currentPos < footerStart) {
                raf.seek(footerStart);
            }
            raf.write(footer);
        }
        return true;
    }

    private static byte[] buildMinimalParquet(byte[] footerWithTail, int footerLen) {
        byte[] footerOnly = new byte[footerLen];
        System.arraycopy(footerWithTail, 0, footerOnly, 0, footerLen);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            out.write(PARQUET_MAGIC);
            out.write(footerOnly);
            ByteBuffer lenBuf = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
            lenBuf.putInt(footerLen);
            out.write(lenBuf.array());
            out.write(PARQUET_MAGIC);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return out.toByteArray();
    }

    @SuppressForbidden(reason = "HTTP HEAD for file size")
    private static long httpFileSize(String url) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
        conn.setRequestMethod("HEAD");
        conn.setConnectTimeout(DOWNLOAD_TIMEOUT_MS);
        conn.setReadTimeout(DOWNLOAD_TIMEOUT_MS);
        try {
            int code = conn.getResponseCode();
            if (code != 200) {
                throw new HttpDownloadRetry.HttpStatusException("HTTP HEAD to [" + url + "] failed with status " + code, code);
            }
            return conn.getContentLengthLong();
        } finally {
            conn.disconnect();
        }
    }

    @SuppressForbidden(reason = "HTTP range request for partial download")
    private static byte[] httpRange(String url, long from, long to) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
        conn.setRequestProperty("Range", "bytes=" + from + "-" + to);
        conn.setConnectTimeout(DOWNLOAD_TIMEOUT_MS);
        conn.setReadTimeout(DOWNLOAD_TIMEOUT_MS);
        try {
            int code = conn.getResponseCode();
            if (code != 206 && code != 200) {
                throw new HttpDownloadRetry.HttpStatusException("HTTP range request to [" + url + "] failed with status " + code, code);
            }
            try (InputStream in = conn.getInputStream()) {
                return in.readAllBytes();
            }
        } finally {
            conn.disconnect();
        }
    }

    private static OutputFile createFileOutputFile(Path path) {
        return new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) throws IOException {
                java.io.OutputStream fos = Files.newOutputStream(path);
                return new PositionOutputStream() {
                    private long position = 0;

                    @Override
                    public long getPos() {
                        return position;
                    }

                    @Override
                    public void write(int b) throws IOException {
                        fos.write(b);
                        position++;
                    }

                    @Override
                    public void write(byte[] b, int off, int len) throws IOException {
                        fos.write(b, off, len);
                        position += len;
                    }

                    @Override
                    public void close() throws IOException {
                        fos.close();
                        super.close();
                    }
                };
            }

            @Override
            public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
                return create(blockSizeHint);
            }

            @Override
            public boolean supportsBlockSize() {
                return false;
            }

            @Override
            public long defaultBlockSize() {
                return 0;
            }
        };
    }

    /**
     * An {@link InputFile} backed by a byte array, for reading Parquet metadata
     * from data assembled in memory from HTTP range requests.
     */
    static class ByteArrayInputFile implements InputFile {
        private final byte[] data;

        ByteArrayInputFile(byte[] data) {
            this.data = data;
        }

        @Override
        public long getLength() {
            return data.length;
        }

        @Override
        public SeekableInputStream newStream() {
            return new SeekableByteArrayInputStream(data);
        }
    }

    /**
     * An {@link InputFile} backed by a file on disk, for streaming Parquet data
     * without loading the entire file into memory.
     */
    @SuppressForbidden(reason = "Reading Parquet files from disk")
    static class FileInputFile implements InputFile {
        private final Path path;

        FileInputFile(Path path) {
            this.path = path;
        }

        @Override
        public long getLength() throws IOException {
            return Files.size(path);
        }

        @Override
        public SeekableInputStream newStream() throws IOException {
            return new SeekableFileInputStream(path);
        }
    }

    @SuppressForbidden(reason = "RandomAccessFile for seekable Parquet reading")
    private static class SeekableFileInputStream extends SeekableInputStream {
        private final RandomAccessFile raf;

        SeekableFileInputStream(Path path) throws IOException {
            this.raf = new RandomAccessFile(path.toFile(), "r");
        }

        @Override
        public long getPos() throws IOException {
            return raf.getFilePointer();
        }

        @Override
        public void seek(long newPos) throws IOException {
            raf.seek(newPos);
        }

        @Override
        public int read() throws IOException {
            return raf.read();
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return raf.read(b, off, len);
        }

        @Override
        public int read(ByteBuffer buf) throws IOException {
            byte[] temp = new byte[buf.remaining()];
            int n = raf.read(temp);
            if (n > 0) {
                buf.put(temp, 0, n);
            }
            return n;
        }

        @Override
        public void readFully(byte[] bytes) throws IOException {
            raf.readFully(bytes);
        }

        @Override
        public void readFully(byte[] bytes, int start, int len) throws IOException {
            raf.readFully(bytes, start, len);
        }

        @Override
        public void readFully(ByteBuffer buf) throws IOException {
            byte[] temp = new byte[buf.remaining()];
            raf.readFully(temp);
            buf.put(temp);
        }

        @Override
        public void close() throws IOException {
            raf.close();
        }
    }

    private static class SeekableByteArrayInputStream extends SeekableInputStream {
        private final byte[] data;
        private int pos;

        SeekableByteArrayInputStream(byte[] data) {
            this.data = data;
            this.pos = 0;
        }

        @Override
        public long getPos() {
            return pos;
        }

        @Override
        public void seek(long newPos) {
            this.pos = (int) newPos;
        }

        @Override
        public int read() {
            if (pos >= data.length) {
                return -1;
            }
            return data[pos++] & 0xFF;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            if (pos >= data.length) {
                return -1;
            }
            int available = Math.min(len, data.length - pos);
            System.arraycopy(data, pos, b, off, available);
            pos += available;
            return available;
        }

        @Override
        public int read(ByteBuffer buf) {
            if (pos >= data.length) {
                return -1;
            }
            int available = Math.min(buf.remaining(), data.length - pos);
            buf.put(data, pos, available);
            pos += available;
            return available;
        }

        @Override
        public void readFully(byte[] bytes) throws IOException {
            readFully(bytes, 0, bytes.length);
        }

        @Override
        public void readFully(byte[] bytes, int start, int len) throws IOException {
            if (pos + len > data.length) {
                throw new EOFException();
            }
            System.arraycopy(data, pos, bytes, start, len);
            pos += len;
        }

        @Override
        public void readFully(ByteBuffer buf) throws IOException {
            int len = buf.remaining();
            if (pos + len > data.length) {
                throw new EOFException();
            }
            buf.put(data, pos, len);
            pos += len;
        }
    }
}
