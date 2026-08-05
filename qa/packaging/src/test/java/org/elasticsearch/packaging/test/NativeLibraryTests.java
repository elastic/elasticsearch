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
import org.elasticsearch.packaging.util.ServerUtils;
import org.elasticsearch.packaging.util.docker.DockerRun;

import java.util.Map;

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
