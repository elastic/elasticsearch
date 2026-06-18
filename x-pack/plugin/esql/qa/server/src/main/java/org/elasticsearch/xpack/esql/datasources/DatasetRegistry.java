/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

/**
 * Class-scoped, idempotent registry of {@code data_source}/{@code dataset} pairs for the
 * {@code FROM <dataset>} spec-test path. Datasets and data sources are {@code ProjectCustom} cluster
 * metadata that survive the REST framework's per-class index wipe, so registration is cached by a
 * content signature: re-registering the same {@code (name, type, settings)} is a no-op, while a
 * changed signature re-issues the create-or-replace PUT. Call {@link #cleanup(RestClient)} from an
 * {@code @AfterClass} hook to drop everything this registry created.
 *
 * <p>The registry is deliberately static: each spec IT runs in its own Gradle JVM fork, and
 * {@link #cleanup(RestClient)} clears the caches, so sequential suites in one JVM stay isolated.
 */
public final class DatasetRegistry {

    /** data_source name -&gt; content signature of the last successful PUT. */
    private static final Map<String, String> dataSources = new LinkedHashMap<>();
    /** dataset name -&gt; content signature of the last successful PUT. */
    private static final Map<String, String> datasets = new LinkedHashMap<>();

    private DatasetRegistry() {}

    /**
     * Ensures a {@code data_source} named {@code name} of the given {@code type} (e.g. {@code s3}) with
     * {@code settings} exists, issuing {@code PUT /_query/data_source/<name>} only when the content
     * signature differs from the cached one. Returns {@code name} for chaining into
     * {@link #ensureDataset}.
     */
    public static synchronized String ensureDataSource(RestClient client, String name, String type, Map<String, Object> settings)
        throws IOException {
        String signature = type + "|" + settings;
        if (signature.equals(dataSources.get(name)) == false) {
            Request req = new Request("PUT", "/_query/data_source/" + name);
            try (XContentBuilder b = jsonBuilder()) {
                b.startObject().field("type", type);
                if (settings.isEmpty() == false) {
                    b.field("settings", settings);
                }
                b.endObject();
                req.setJsonEntity(Strings.toString(b));
            }
            assertOk(client.performRequest(req), "PUT data_source [" + name + "]");
            dataSources.put(name, signature);
        }
        return name;
    }

    /**
     * Ensures a {@code dataset} named {@code name} bound to {@code dataSource} + {@code resource} with the
     * given format {@code settings} exists, issuing {@code PUT /_query/dataset/<name>} only when the
     * content signature differs from the cached one. The owning {@code data_source} must already exist
     * (register it first via {@link #ensureDataSource}); the CRUD layer rejects a dataset whose parent is
     * missing.
     */
    public static synchronized void ensureDataset(
        RestClient client,
        String name,
        String dataSource,
        String resource,
        Map<String, Object> settings
    ) throws IOException {
        String signature = dataSource + "|" + resource + "|" + settings;
        if (signature.equals(datasets.get(name))) {
            return;
        }
        Request req = new Request("PUT", "/_query/dataset/" + name);
        try (XContentBuilder b = jsonBuilder()) {
            b.startObject().field("data_source", dataSource).field("resource", resource);
            if (settings.isEmpty() == false) {
                b.field("settings", settings);
            }
            b.endObject();
            req.setJsonEntity(Strings.toString(b));
        }
        assertOk(client.performRequest(req), "PUT dataset [" + name + "]");
        datasets.put(name, signature);
    }

    /**
     * Deletes everything this registry created, datasets first so the data-source deletes do not 409 on
     * a still-referenced parent, then clears the caches. Swallows 404s so a partially-set-up suite can be
     * swept cleanly.
     */
    public static synchronized void cleanup(RestClient client) throws IOException {
        for (String name : datasets.keySet()) {
            deleteIgnoringMissing(client, "/_query/dataset/" + name);
        }
        datasets.clear();
        for (String name : dataSources.keySet()) {
            deleteIgnoringMissing(client, "/_query/data_source/" + name);
        }
        dataSources.clear();
    }

    private static void deleteIgnoringMissing(RestClient client, String path) throws IOException {
        try {
            client.performRequest(new Request("DELETE", path));
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() != 404) {
                throw e;
            }
        }
    }

    private static void assertOk(Response response, String what) throws IOException {
        int status = response.getStatusLine().getStatusCode();
        if (status != 200) {
            throw new IOException(what + " returned unexpected status [" + status + "]");
        }
    }
}
