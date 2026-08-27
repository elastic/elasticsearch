/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.packaging.test;

import org.apache.http.client.fluent.Request;
import org.apache.http.entity.ContentType;
import org.elasticsearch.packaging.util.FileUtils;
import org.elasticsearch.packaging.util.Platforms;
import org.elasticsearch.packaging.util.ServerUtils;
import org.elasticsearch.packaging.util.docker.Docker;
import org.elasticsearch.packaging.util.docker.DockerRun;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.packaging.util.docker.Docker.runContainer;
import static org.elasticsearch.packaging.util.docker.DockerRun.builder;
import static org.hamcrest.CoreMatchers.containsString;

/**
 * Packaging tests that verify native libraries load and function correctly on each supported platform.
 * <p>
 * These tests exercise features that depend on platform-specific native libraries (loaded via Panama/FFM),
 * ensuring that the libraries are compatible with the glibc (or equivalent) on the target OS. A glibc
 * version mismatch would cause the library to fail to load at runtime, which these tests are designed to catch.
 */
public class NativeLibraryTests extends PackagingTestCase {

    private static final Map<String, String> SECURITY_DISABLED_SETTINGS = Map.of(
        "xpack.security.enabled",
        "false",
        "xpack.security.http.ssl.enabled",
        "false",
        "xpack.security.enrollment.enabled",
        "false",
        "discovery.type",
        "single-node"
    );

    public void test10Install() throws Exception {
        install();
    }

    /**
     * Verifies that the native zstd library loads and can compress/decompress stored fields.
     * <p>
     * The {@code best_compression} codec uses {@code Zstd814StoredFieldsFormat}, which compresses
     * stored fields via the native {@code libzstd.so} (or platform equivalent). If the native library
     * cannot be loaded (e.g. due to a glibc version incompatibility), index creation or document
     * indexing will fail.
     */
    public void test20ZstdCompression() throws Exception {
        configureAndStart(SECURITY_DISABLED_SETTINGS);

        try {
            // Create an index with best_compression codec, which uses native zstd for stored fields
            ServerUtils.makeRequest(
                Request.Put("http://localhost:9200/zstd_test")
                    .bodyString(
                        "{\"settings\": {\"index.codec\": \"best_compression\", \"number_of_replicas\": 0}}",
                        ContentType.APPLICATION_JSON
                    )
            );

            // Index a document — this exercises zstd compression of the stored _source field
            ServerUtils.makeRequest(
                Request.Post("http://localhost:9200/zstd_test/_doc/1?refresh=true")
                    .bodyString("{\"message\": \"zstd native library smoke test for packaging\"}", ContentType.APPLICATION_JSON)
            );

            // Retrieve the document — this exercises zstd decompression of the stored _source field
            String response = ServerUtils.makeRequest(Request.Get("http://localhost:9200/zstd_test/_doc/1"));
            assertThat(response, containsString("zstd native library smoke test for packaging"));
        } finally {
            stopElasticsearch();
        }
    }

    /**
     * Verifies that the native simdvec library (libvec.so/libvec.dylib) loads and can perform vector similarity
     * scoring.
     * <p>
     * A {@code dense_vector} field with HNSW indexing uses the native vector scorer from {@code libvec}
     * for computing vector distances during kNN search. If the native library cannot be loaded (e.g. due to
     * a glibc version incompatibility), the search will fall back to the Java implementation or fail entirely.
     * This test indexes vectors and performs a kNN search to exercise the native scoring path.
     * <p>
     * On Linux and macOS (where native simdvec is supported), this test additionally asserts that the native
     * library was loaded successfully by checking for the {@code vec_caps=N} log line (where N > 0) emitted
     * by the simdvec library during startup.
     */
    public void test30SimdVecKnnSearch() throws Exception {
        configureAndStart(SECURITY_DISABLED_SETTINGS);

        try {
            // Create an index with a dense_vector field using plain HNSW (no quantization).
            // Explicitly setting "type": "hnsw" avoids the default int8_hnsw quantization,
            // ensuring the native float32 vector scorer in libvec is used for distance computation.
            ServerUtils.makeRequest(Request.Put("http://localhost:9200/simdvec_test").bodyString("""
                {
                  "settings": {"number_of_replicas": 0, "number_of_shards": 1},
                  "mappings": {
                    "properties": {
                      "vector": {
                        "type": "dense_vector",
                        "dims": 3,
                        "index": true,
                        "similarity": "l2_norm",
                        "index_options": {"type": "hnsw"}
                      },
                      "name": {"type": "keyword"}
                    }
                  }
                }""", ContentType.APPLICATION_JSON));

            // Index documents with vectors
            ServerUtils.makeRequest(
                Request.Post("http://localhost:9200/simdvec_test/_doc/1")
                    .bodyString("{\"vector\": [1.0, 2.0, 3.0], \"name\": \"first\"}", ContentType.APPLICATION_JSON)
            );
            ServerUtils.makeRequest(
                Request.Post("http://localhost:9200/simdvec_test/_doc/2")
                    .bodyString("{\"vector\": [4.0, 5.0, 6.0], \"name\": \"second\"}", ContentType.APPLICATION_JSON)
            );
            ServerUtils.makeRequest(
                Request.Post("http://localhost:9200/simdvec_test/_doc/3?refresh=true")
                    .bodyString("{\"vector\": [7.0, 8.0, 9.0], \"name\": \"third\"}", ContentType.APPLICATION_JSON)
            );

            // Force merge to a single segment to ensure the HNSW graph is built and the native scorer is used
            ServerUtils.makeRequest(Request.Post("http://localhost:9200/simdvec_test/_forcemerge?max_num_segments=1"));

            // Perform a kNN search — this exercises native vector distance scoring via libvec
            String response = ServerUtils.makeRequest(Request.Post("http://localhost:9200/simdvec_test/_search").bodyString("""
                {
                  "knn": {
                    "field": "vector",
                    "query_vector": [1.0, 2.0, 3.0],
                    "k": 1,
                    "num_candidates": 3
                  }
                }""", ContentType.APPLICATION_JSON));

            // The nearest neighbor to [1,2,3] should be the document with vector [1,2,3]
            assertThat(response, containsString("\"_id\":\"1\""));
            assertThat(response, containsString("\"first\""));

            // On Linux and macOS (where native simdvec is supported), verify that libvec loaded and vec_caps > 0.
            // The vec_caps log line is emitted during NativeAccess initialization at node startup.
            if (Platforms.LINUX || Platforms.DARWIN) {
                String logs = getElasticsearchLogs();
                Matcher matcher = Pattern.compile("vec_caps=(\\d+)").matcher(logs);
                assertTrue("Expected vec_caps=N log line indicating simdvec library loaded, but not found in logs", matcher.find());
                int vecCaps = Integer.parseInt(matcher.group(1));
                assertTrue("Expected vec_caps > 0, indicating native simdvec is operational, but got: " + vecCaps, vecCaps > 0);
            }
        } finally {
            stopElasticsearch();
        }
    }

    /**
     * Returns the Elasticsearch startup logs, handling both Docker and non-Docker distributions.
     */
    private String getElasticsearchLogs() {
        if (distribution().isDocker()) {
            return Docker.getContainerLogs().stdout();
        }
        return FileUtils.slurpAllLogs(installation.logs, "elasticsearch.log", "*.log.gz");
    }

    private void configureAndStart(Map<String, String> settings) throws Exception {
        if (distribution().isDocker()) {
            DockerRun dockerRun = builder();
            settings.forEach(dockerRun::envVar);
            installation = runContainer(distribution(), dockerRun);
        } else {
            for (var setting : settings.entrySet()) {
                ServerUtils.addSettingToExistingConfiguration(installation.config, setting.getKey(), setting.getValue());
            }
            ServerUtils.removeSettingFromExistingConfiguration(installation.config, "cluster.initial_master_nodes");
        }

        startElasticsearch();
        ServerUtils.waitForElasticsearch(installation);
    }
}
