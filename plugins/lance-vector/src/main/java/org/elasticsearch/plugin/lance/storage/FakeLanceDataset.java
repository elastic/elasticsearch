/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.lance.storage;

import org.elasticsearch.core.PathUtils;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Locale;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Simple in-process dataset used for tests. Loads vectors from a JSON file shaped as:
 *
 * [
 *   { "id": "doc1", "vector": [0.1, 0.2, ...] },
 *   { "id": "doc2", "vector": [0.3, 0.1, ...] }
 * ]
 *
 * Supports embedded:, file://, and oss:// URIs.
 * <p>
 * Implements the {@link LanceDataset} interface for compatibility with the
 * production Lance integration.
 */
public class FakeLanceDataset implements LanceDataset {
    public record Entry(String id, float[] vector) {}

    private final String uri;
    private final List<Entry> entries;
    private final int dims;

    private FakeLanceDataset(String uri, List<Entry> entries, int dims) {
        this.uri = uri;
        this.entries = entries;
        this.dims = dims;
    }

    public static FakeLanceDataset load(String uri, int expectedDims) throws IOException {
        InputStream inputStream;

        if (uri.startsWith("embedded:")) {
            // Load from classpath resource (for POC/testing)
            String resourceName = uri.substring("embedded:".length());
            // Try with and without leading slash
            inputStream = FakeLanceDataset.class.getClassLoader().getResourceAsStream(resourceName);
            if (inputStream == null) {
                inputStream = FakeLanceDataset.class.getResourceAsStream("/" + resourceName);
            }
            if (inputStream == null) {
                throw new IOException("Embedded resource not found: " + resourceName);
            }
        } else if (OssStorageAdapter.isOssUri(uri)) {
            // Load from OSS
            OssStorageAdapter ossAdapter = new OssStorageAdapter();
            inputStream = ossAdapter.readObject(uri);
        } else {
            // Load from local file
            Path path = PathUtils.get(Objects.requireNonNull(uri.replaceFirst("^file://", "")));
            if (Files.exists(path) == false) {
                throw new IOException("Fake Lance dataset not found at " + path);
            }
            inputStream = Files.newInputStream(path);
        }

        try (
            XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(XContentParserConfiguration.EMPTY, inputStream)
        ) {
            List<Entry> loaded = new ArrayList<>();
            if (parser.nextToken() != XContentParser.Token.START_ARRAY) {
                throw new IOException("Expected JSON array for dataset");
            }
            while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                Map<String, Object> obj = parser.map();
                String id = Objects.requireNonNull((String) obj.get("id"), "id missing");
                Object vectorNode = obj.get("vector");
                if (vectorNode instanceof List<?> vectorList) {
                    float[] vec = new float[vectorList.size()];
                    for (int i = 0; i < vectorList.size(); i++) {
                        vec[i] = ((Number) vectorList.get(i)).floatValue();
                    }
                    loaded.add(new Entry(id, vec));
                } else {
                    throw new IOException("vector missing for id=" + id);
                }
            }
            if (loaded.isEmpty()) {
                throw new IOException("Dataset empty at " + uri);
            }
            int dims = loaded.get(0).vector().length;
            if (expectedDims != 0 && dims != expectedDims) {
                throw new IOException("Expected dims=" + expectedDims + " but dataset has dims=" + dims);
            }
            return new FakeLanceDataset(uri, List.copyOf(loaded), dims);
        }
    }

    @Override
    public int dims() {
        return dims;
    }

    @Override
    public long size() {
        return entries.size();
    }

    @Override
    public String uri() {
        return uri;
    }

    @Override
    public List<Candidate> search(float[] query, int numCandidates, String similarity) {
        List<Candidate> scored = entries.stream()
            .map(e -> new Candidate(e.id(), score(query, e.vector(), similarity)))
            .sorted(Comparator.comparing(Candidate::score).reversed())
            .limit(numCandidates)
            .collect(Collectors.toList());
        return scored;
    }

    private float score(float[] query, float[] vector, String similarity) {
        if (query.length != vector.length) {
            throw new IllegalArgumentException("Vector dims mismatch");
        }
        switch (similarity.toLowerCase(Locale.ROOT)) {
            case "cosine":
                return cosine(query, vector);
            case "dot_product":
            case "dot":
                return dot(query, vector);
            default:
                return cosine(query, vector);
        }
    }

    private static float dot(float[] a, float[] b) {
        float s = 0f;
        for (int i = 0; i < a.length; i++) {
            s += a[i] * b[i];
        }
        return s;
    }

    private static float cosine(float[] a, float[] b) {
        float dot = 0f;
        float normA = 0f;
        float normB = 0f;
        for (int i = 0; i < a.length; i++) {
            dot += a[i] * b[i];
            normA += a[i] * a[i];
            normB += b[i] * b[i];
        }
        if (normA == 0f || normB == 0f) {
            return 0f;
        }
        return (float) (dot / (Math.sqrt(normA) * Math.sqrt(normB)));
    }
}
