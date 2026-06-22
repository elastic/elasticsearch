/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

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
 * {@link #cleanup(RestClient)} clears the caches, so sequential suites in one JVM stay isolated. When a
 * suite cannot run {@link #cleanup(RestClient)} (a broken cluster, or a failure before the
 * {@code @AfterClass} hook), it must still call {@link #clearCaches()} so a later suite in the same fork
 * starts from an empty cache rather than skipping a needed PUT against a cluster that no longer holds the
 * custom.
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
     * format options in the {@code withJson} object (the {@code WITH {...}} JSON, or {@code null} for
     * none) exists, issuing {@code PUT /_query/dataset/<name>} only when the content signature differs
     * from the cached one. The owning {@code data_source} must already exist (register it first via
     * {@link #ensureDataSource}); the CRUD layer rejects a dataset whose parent is missing.
     * <p>
     * The signature is keyed off the raw {@code withJson} so the JSON is parsed only on a cache miss, not
     * on every spec invocation.
     */
    public static synchronized void ensureDataset(RestClient client, String name, String dataSource, String resource, String withJson)
        throws IOException {
        String signature = dataSource + "|" + resource + "|" + withJson;
        if (signature.equals(datasets.get(name)) == false) {
            putDataset(client, name, dataSource, resource, parseSettings(withJson));
            datasets.put(name, signature);
        }
    }

    /** Parses a {@code dataset:} directive's {@code WITH {...}} JSON into a settings map ({@code null} maps to empty). */
    private static Map<String, Object> parseSettings(String withJson) throws IOException {
        if (withJson == null) {
            return Map.of();
        }
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, withJson)) {
            return parser.map();
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
     * Clears the static caches without issuing any REST calls. Call this from an {@code @AfterClass}
     * {@code finally} so a suite that could not run {@link #cleanup(RestClient)} (broken cluster, or a
     * failure that aborted cleanup) does not leave stale entries that would make a later suite in the same
     * JVM fork skip a needed PUT.
     */
    public static synchronized void clearCaches() {
        datasets.clear();
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

    private static void assertOk(Response response, String what) throws IOException {
        int status = response.getStatusLine().getStatusCode();
        if (status != 200) {
            String body = response.getEntity() == null ? "<no body>" : EntityUtils.toString(response.getEntity());
            throw new AssertionError(what + " returned unexpected status [" + status + "]: " + body);
        }
    }
}
