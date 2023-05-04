/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.packageloader.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.Tuple;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static java.net.HttpURLConnection.HTTP_MOVED_PERM;
import static java.net.HttpURLConnection.HTTP_MOVED_TEMP;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_SEE_OTHER;

/**
 * Helper class for downloading pre-trained Elastic models, available on ml-models.elastic.co or as file
 */
final class ModelLoaderUtils {

    public static String METADATA_FILE_EXTENSION = ".metadata.json";
    public static String MODEL_FILE_EXTENSION = ".pt";

    private static ByteSizeValue VOCABULARY_SIZE_LIMIT = new ByteSizeValue(10, ByteSizeUnit.MB);
    private static final String VOCABULARY = "vocabulary";
    private static final String MERGES = "merges";

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

    static InputStream getInputStreamFromModelRepository(URI uri) throws IOException {
        String scheme = uri.getScheme().toLowerCase(Locale.ROOT);

        // if you add a scheme here, also add it to the bootstrap check in {@link MachineLearningPackageLoader#validateModelRepository}
        switch (scheme) {
            case "http":
            case "https":
                return getHttpOrHttpsInputStream(uri);
            case "file":
                return getFileInputStream(uri);
            default:
                throw new IllegalArgumentException("unsupported scheme");
        }
    }

    static Tuple<List<String>, List<String>> loadVocabulary(URI uri) {
        try {
            InputStream vocabInputStream = getInputStreamFromModelRepository(uri);

            if (uri.getPath().endsWith(".json")) {
                XContentParser sourceParser = XContentType.JSON.xContent()
                    .createParser(
                        XContentParserConfiguration.EMPTY,
                        Streams.limitStream(vocabInputStream, VOCABULARY_SIZE_LIMIT.getBytes())
                    );
                Map<String, List<Object>> vocabAndMerges = sourceParser.map(HashMap::new, XContentParser::list);

                List<String> vocabulary = vocabAndMerges.containsKey(VOCABULARY)
                    ? vocabAndMerges.get(VOCABULARY).stream().map(Object::toString).collect(Collectors.toList())
                    : Collections.emptyList();
                List<String> merges = vocabAndMerges.containsKey(MERGES)
                    ? vocabAndMerges.get(MERGES).stream().map(Object::toString).collect(Collectors.toList())
                    : Collections.emptyList();

                return Tuple.tuple(vocabulary, merges);
            }

            throw new IllegalArgumentException("unknown format vocabulary file format");
        } catch (Exception e) {
            throw new RuntimeException("Failed to load vocabulary file", e);
        }
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
    private static InputStream getHttpOrHttpsInputStream(URI uri) throws IOException {

        assert uri.getUserInfo() == null : "URI's with credentials are not supported";

        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }

        PrivilegedAction<InputStream> privilegedHttpReader = () -> {
            try {
                HttpURLConnection conn = (HttpURLConnection) uri.toURL().openConnection();
                switch (conn.getResponseCode()) {
                    case HTTP_OK:
                        return conn.getInputStream();
                    case HTTP_MOVED_PERM:
                    case HTTP_MOVED_TEMP:
                    case HTTP_SEE_OTHER:
                        throw new IllegalStateException("redirects aren't supported yet");
                    case HTTP_NOT_FOUND:
                        throw new ResourceNotFoundException("{} not found", uri);
                    default:
                        int responseCode = conn.getResponseCode();
                        throw new ElasticsearchStatusException("error during downloading {}", RestStatus.fromCode(responseCode), uri);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };

        return AccessController.doPrivileged(privilegedHttpReader);
    }

    @SuppressWarnings("'java.lang.SecurityManager' is deprecated and marked for removal ")
    @SuppressForbidden(reason = "we need load model data from a file")
    private static InputStream getFileInputStream(URI uri) {

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

}
