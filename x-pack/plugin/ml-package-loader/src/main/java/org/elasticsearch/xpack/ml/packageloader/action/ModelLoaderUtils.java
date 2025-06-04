/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.packageloader.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.security.AccessController;
import java.security.MessageDigest;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static java.net.HttpURLConnection.HTTP_MOVED_PERM;
import static java.net.HttpURLConnection.HTTP_MOVED_TEMP;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_PARTIAL;
import static java.net.HttpURLConnection.HTTP_SEE_OTHER;

/**
 * Helper class for downloading pre-trained Elastic models, available on ml-models.elastic.co or as file
 */
final class ModelLoaderUtils {

    private static final Logger logger = LogManager.getLogger(ModelLoaderUtils.class);

    public static String METADATA_FILE_EXTENSION = ".metadata.json";
    public static String MODEL_FILE_EXTENSION = ".pt";

    private static final ByteSizeValue VOCABULARY_SIZE_LIMIT = ByteSizeValue.of(20, ByteSizeUnit.MB);
    private static final String VOCABULARY = "vocabulary";
    private static final String MERGES = "merges";
    private static final String SCORES = "scores";

    record VocabularyParts(List<String> vocab, List<String> merges, List<Double> scores) {}

    // Range in bytes
    record RequestRange(long rangeStart, long rangeEnd, int startPart, int numParts) {
        public String bytesRange() {
            return "bytes=" + rangeStart + "-" + rangeEnd;
        }
    }

    static class HttpStreamChunker {

        record BytesAndPartIndex(BytesArray bytes, int partIndex) {}

        private final InputStream inputStream;
        private final int chunkSize;
        private final AtomicLong totalBytesRead = new AtomicLong();
        private final AtomicInteger currentPart;
        private final int lastPartNumber;
        private final byte[] buf;
        private final RequestRange range;  // TODO debug only

        HttpStreamChunker(URI uri, RequestRange range, int chunkSize) {
            var inputStream = getHttpOrHttpsInputStream(uri, range);
            this.inputStream = inputStream;
            this.chunkSize = chunkSize;
            this.lastPartNumber = range.startPart() + range.numParts();
            this.currentPart = new AtomicInteger(range.startPart());
            this.buf = new byte[chunkSize];
            this.range = range;
        }

        // This ctor exists for testing purposes only.
        HttpStreamChunker(InputStream inputStream, RequestRange range, int chunkSize) {
            this.inputStream = inputStream;
            this.chunkSize = chunkSize;
            this.lastPartNumber = range.startPart() + range.numParts();
            this.currentPart = new AtomicInteger(range.startPart());
            this.buf = new byte[chunkSize];
            this.range = range;
        }

        public boolean hasNext() {
            return currentPart.get() < lastPartNumber;
        }

        public BytesAndPartIndex next() throws IOException {
            int bytesRead = 0;

            while (bytesRead < chunkSize) {
                int read = inputStream.read(buf, bytesRead, chunkSize - bytesRead);
                // EOF??
                if (read == -1) {
                    logger.debug("end of stream, " + bytesRead + " bytes read");
                    break;
                }
                bytesRead += read;
            }

            if (bytesRead > 0) {
                totalBytesRead.addAndGet(bytesRead);
                return new BytesAndPartIndex(new BytesArray(buf, 0, bytesRead), currentPart.getAndIncrement());
            } else {
                logger.warn("Empty part in range " + range + ", current part=" + currentPart.get() + ", last part=" + lastPartNumber);
                return new BytesAndPartIndex(BytesArray.EMPTY, currentPart.get());
            }
        }

        public long getTotalBytesRead() {
            return totalBytesRead.get();
        }

        public int getCurrentPart() {
            return currentPart.get();
        }
    }

    static class InputStreamChunker {

        private final InputStream inputStream;
        private final MessageDigest digestSha256 = MessageDigests.sha256();
        private final int chunkSize;

        private int totalBytesRead = 0;

        InputStreamChunker(InputStream inputStream, int chunkSize) {
            this.inputStream = inputStream;
            this.chunkSize = chunkSize;
        }

        public BytesArray next() throws IOException {
            int bytesRead = 0;
            byte[] buf = new byte[chunkSize];

            while (bytesRead < chunkSize) {
                int read = inputStream.read(buf, bytesRead, chunkSize - bytesRead);
                // EOF??
                if (read == -1) {
                    break;
                }
                bytesRead += read;
            }
            digestSha256.update(buf, 0, bytesRead);
            totalBytesRead += bytesRead;

            return new BytesArray(buf, 0, bytesRead);
        }

        public String getSha256() {
            return MessageDigests.toHexString(digestSha256.digest());
        }

        public int getTotalBytesRead() {
            return totalBytesRead;
        }
    }

    static InputStream getInputStreamFromModelRepository(URI uri) {
        String scheme = uri.getScheme().toLowerCase(Locale.ROOT);

        // if you add a scheme here, also add it to the bootstrap check in {@link MachineLearningPackageLoader#validateModelRepository}
        switch (scheme) {
            case "http":
            case "https":
                return getHttpOrHttpsInputStream(uri, null);
            case "file":
                return getFileInputStream(uri);
            default:
                throw new IllegalArgumentException("unsupported scheme");
        }
    }

    static boolean uriIsFile(URI uri) {
        String scheme = uri.getScheme().toLowerCase(Locale.ROOT);
        return "file".equals(scheme);
    }

    static VocabularyParts loadVocabulary(URI uri) {
        if (uri.getPath().endsWith(".json")) {
            try (InputStream vocabInputStream = getInputStreamFromModelRepository(uri)) {
                return parseVocabParts(vocabInputStream);
            } catch (Exception e) {
                throw new ElasticsearchException("Failed to load vocabulary file", e);
            }
        }

        throw new IllegalArgumentException("unknown format vocabulary file format");
    }

    // visible for testing
    static VocabularyParts parseVocabParts(InputStream vocabInputStream) throws IOException {
        Map<String, List<Object>> vocabParts;
        try (
            XContentParser sourceParser = XContentType.JSON.xContent()
                .createParser(XContentParserConfiguration.EMPTY, Streams.limitStream(vocabInputStream, VOCABULARY_SIZE_LIMIT.getBytes()))
        ) {
            vocabParts = sourceParser.map(HashMap::new, XContentParser::list);
        }

        List<String> vocabulary = vocabParts.containsKey(VOCABULARY)
            ? vocabParts.get(VOCABULARY).stream().map(Object::toString).collect(Collectors.toList())
            : List.of();
        List<String> merges = vocabParts.containsKey(MERGES)
            ? vocabParts.get(MERGES).stream().map(Object::toString).collect(Collectors.toList())
            : List.of();
        List<Double> scores = vocabParts.containsKey(SCORES)
            ? vocabParts.get(SCORES).stream().map(o -> (Double) o).collect(Collectors.toList())
            : List.of();

        return new VocabularyParts(vocabulary, merges, scores);
    }

    static URI resolvePackageLocation(String repository, String artefact) throws URISyntaxException {
        URI baseUri = new URI(repository.endsWith("/") ? repository : repository + "/").normalize();
        URI resolvedUri = baseUri.resolve(artefact).normalize();

        if (Strings.isNullOrEmpty(baseUri.getScheme())) {
            throw new IllegalArgumentException("Repository must contain a scheme");
        }

        if (baseUri.getScheme().equals(resolvedUri.getScheme()) == false) {
            throw new IllegalArgumentException("Illegal schema change in package location");
        }

        if (resolvedUri.getPath().startsWith(baseUri.getPath()) == false) {
            throw new IllegalArgumentException("Illegal path in package location");
        }

        return baseUri.resolve(artefact);
    }

    private ModelLoaderUtils() {}

    @SuppressWarnings("'java.lang.SecurityManager' is deprecated and marked for removal ")
    @SuppressForbidden(reason = "we need socket connection to download")
    private static InputStream getHttpOrHttpsInputStream(URI uri, @Nullable RequestRange range) {

        assert uri.getUserInfo() == null : "URI's with credentials are not supported";

        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }

        PrivilegedAction<InputStream> privilegedHttpReader = () -> {
            try {
                HttpURLConnection conn = (HttpURLConnection) uri.toURL().openConnection();
                if (range != null) {
                    conn.setRequestProperty("Range", range.bytesRange());
                }
                switch (conn.getResponseCode()) {
                    case HTTP_OK:
                    case HTTP_PARTIAL:
                        return conn.getInputStream();

                    case HTTP_MOVED_PERM:
                    case HTTP_MOVED_TEMP:
                    case HTTP_SEE_OTHER:
                        throw new IllegalStateException("redirects aren't supported yet");
                    case HTTP_NOT_FOUND:
                        throw new ResourceNotFoundException("{} not found", uri);
                    case 416: // Range not satisfiable, for some reason not in the list of constants
                        throw new IllegalStateException("Invalid request range [" + range.bytesRange() + "]");
                    default:
                        int responseCode = conn.getResponseCode();
                        throw new ElasticsearchStatusException(
                            "error during downloading {}. Got response code {}",
                            RestStatus.fromCode(responseCode),
                            uri,
                            responseCode
                        );
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };

        return AccessController.doPrivileged(privilegedHttpReader);
    }

    @SuppressWarnings("'java.lang.SecurityManager' is deprecated and marked for removal ")
    @SuppressForbidden(reason = "we need load model data from a file")
    static InputStream getFileInputStream(URI uri) {

        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }

        PrivilegedAction<InputStream> privilegedFileReader = () -> {
            File file = new File(uri);
            if (file.exists() == false) {
                throw new ResourceNotFoundException("{} not found", uri);
            }

            try {
                return Files.newInputStream(file.toPath());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };

        return AccessController.doPrivileged(privilegedFileReader);
    }

    /**
     * Split a stream of size {@code sizeInBytes} into {@code numberOfStreams} +1
     * ranges aligned on {@code chunkSizeBytes} boundaries. Each range contains a
     * whole number of chunks.
     * All ranges except the final range will be split approximately evenly
     * (in terms of number of chunks not the byte size), the final range split
     * is for the single final chunk and will be no more than {@code chunkSizeBytes}
     * in size. The separate range for the final chunk is because when streaming and
     * uploading a large model definition, writing the last part has to handled
     * as a special case.
     * Fewer ranges may be returned in case the stream size is too small.
     * @param sizeInBytes The total size of the stream
     * @param numberOfStreams Divide the bulk of the size into this many streams.
     * @param chunkSizeBytes The size of each chunk
     * @return List of {@code numberOfStreams} + 1  or fewer ranges.
     */
    static List<RequestRange> split(long sizeInBytes, int numberOfStreams, long chunkSizeBytes) {
        int numberOfChunks = (int) ((sizeInBytes + chunkSizeBytes - 1) / chunkSizeBytes);

        int numberOfRanges = numberOfStreams + 1;
        if (numberOfStreams > numberOfChunks) {
            numberOfRanges = numberOfChunks;
        }
        var ranges = new ArrayList<RequestRange>();

        int baseChunksPerRange = (numberOfChunks - 1) / (numberOfRanges - 1);
        int remainder = (numberOfChunks - 1) % (numberOfRanges - 1);
        long startOffset = 0;
        int startChunkIndex = 0;

        for (int i = 0; i < numberOfRanges - 1; i++) {
            int numChunksInRange = (i < remainder) ? baseChunksPerRange + 1 : baseChunksPerRange;

            long rangeEnd = startOffset + (((long) numChunksInRange) * chunkSizeBytes) - 1; // range index is 0 based

            ranges.add(new RequestRange(startOffset, rangeEnd, startChunkIndex, numChunksInRange));
            startOffset = rangeEnd + 1; // range is inclusive start and end
            startChunkIndex += numChunksInRange;
        }

        // The final range is a single chunk the end of which should not exceed sizeInBytes
        long rangeEnd = Math.min(sizeInBytes, startOffset + (baseChunksPerRange * chunkSizeBytes)) - 1;
        ranges.add(new RequestRange(startOffset, rangeEnd, startChunkIndex, 1));

        return ranges;
    }
}
