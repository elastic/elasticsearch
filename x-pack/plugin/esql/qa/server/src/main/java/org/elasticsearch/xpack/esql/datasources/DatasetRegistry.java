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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * Class-scoped, idempotent registry of {@code data_source}/{@code dataset} pairs for the
 * {@code FROM <dataset>} spec-test path, plus the raw {@code PUT}/{@code DELETE} verbs shared with test
 * bases that manage their own dataset lifecycle.
 *
 * <p>Datasets and data sources are {@code ProjectCustom} cluster metadata, and none of
 * {@link org.elasticsearch.test.rest.ESRestTestCase}'s between-test cleanup steps remove them: the wipe
 * deletes indices, data streams, views, templates, snapshots, cluster settings and ILM/CCR/shutdown
 * metadata and issues {@code POST /_features/_reset}, but ES|QL registers no system-feature reset hook,
 * so these customs persist for the whole JVM until explicitly deleted. Registration is therefore safely
 * cached by a content signature: re-registering the same {@code (name, type, settings)} is a no-op,
 * while a changed signature re-issues the create-or-replace PUT. Call {@link #cleanup(RestClient)} from
 * an {@code @AfterClass} hook to drop everything this registry created and clear the caches.
 *
 * <p>The registry is deliberately static: each spec IT runs in its own Gradle JVM fork, and
 * {@link #cleanup(RestClient)} clears the caches, so sequential suites in one JVM stay isolated.
 *
 * <p>The survival invariant above is the load-bearing assumption behind the cache, so
 * {@link #cleanup(RestClient)} fails loudly if a custom this registry recorded as successfully
 * registered has vanished by teardown. That can only happen if the invariant breaks (e.g. a future
 * framework change starts wiping these customs); surfacing it at teardown turns an otherwise
 * non-obvious "FROM &lt;dataset&gt; not found" mid-suite failure into a precise signal.
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
            putDataSource(client, name, type, settings);
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
        if (signature.equals(datasets.get(name)) == false) {
            putDataset(client, name, dataSource, resource, settings);
            datasets.put(name, signature);
        }
    }

    /**
     * Issues an uncached {@code PUT /_query/data_source/<name>} with {@code type} and (optional)
     * {@code settings}. Shared by {@link #ensureDataSource} and by test bases that manage their own
     * data-source lifecycle; callers wanting create-once-per-signature semantics should use
     * {@link #ensureDataSource} instead.
     */
    public static void putDataSource(RestClient client, String name, String type, Map<String, Object> settings) throws IOException {
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
    }

    /**
     * Issues an uncached {@code PUT /_query/dataset/<name>} bound to {@code dataSource} + {@code resource}
     * with the given format {@code settings}. Shared by {@link #ensureDataset} and by test bases that
     * manage their own dataset lifecycle; the owning {@code data_source} must already exist.
     */
    public static void putDataset(RestClient client, String name, String dataSource, String resource, Map<String, Object> settings)
        throws IOException {
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
    }

    /**
     * Deletes everything this registry created, datasets first so the data-source deletes do not 409 on
     * a still-referenced parent, then clears the caches. Every cached entry was recorded only after a
     * successful PUT and is expected to survive the suite (see the class Javadoc), so a missing custom
     * here is treated as a broken survival invariant and fails teardown rather than being swallowed.
     */
    public static synchronized void cleanup(RestClient client) throws IOException {
        for (String name : datasets.keySet()) {
            deleteRegistered(client, "/_query/dataset/" + name);
        }
        datasets.clear();
        for (String name : dataSources.keySet()) {
            deleteRegistered(client, "/_query/data_source/" + name);
        }
        dataSources.clear();
    }

    /**
     * {@code DELETE <path>} that swallows 404s, for ad-hoc sweeps of resources whose existence is not
     * tracked by this registry (e.g. test indices, or datasets a test may or may not have created). The
     * registry's own {@link #cleanup} deliberately does <em>not</em> use this — it expects its tracked
     * customs to still exist.
     */
    public static void deleteIgnoringMissing(RestClient client, String path) throws IOException {
        try {
            client.performRequest(new Request("DELETE", path));
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() != 404) {
                throw e;
            }
        }
    }

    /** Deletes a custom this registry registered; a 404 means it vanished mid-suite — see {@link #cleanup}. */
    private static void deleteRegistered(RestClient client, String path) throws IOException {
        try {
            client.performRequest(new Request("DELETE", path));
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() == 404) {
                throw new AssertionError(
                    "["
                        + path
                        + "] was registered by this suite but is already gone at teardown; the data_source/dataset "
                        + "survival invariant the registry's cache relies on appears to be broken",
                    e
                );
            }
            throw e;
        }
    }

    private static void assertOk(Response response, String what) {
        assertThat(what, response.getStatusLine().getStatusCode(), equalTo(200));
    }
}
