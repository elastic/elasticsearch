/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.client;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.nio.client.HttpAsyncClient;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

/**
 * A {@link RestClient} that records whether any non-read (write) request has been issued through
 * it since {@link #resetWriteOccurred()} was last called. Tests use this to decide whether the
 * cluster needs a real cleanup at the end of a test, or whether the test was effectively
 * read-only and any deferred cleanup can be carried over to the next test.
 *
 * <p>Detection runs synchronously here in {@link #performRequest(Request)} on the test thread —
 * not from inside an async {@code HttpRequestInterceptor} — so there is no I/O-thread
 * re-entrancy or pipeline-ordering hazard.</p>
 */
public final class LazyRefreshRestClient extends RestClient {

    /** Set to true on every non-read request. Reset by callers between tests/phases. */
    private static final AtomicBoolean WRITE_OCCURRED = new AtomicBoolean(false);

    /**
     * Set to true on requests that create a persistent or long-running cluster-side resource
     * (transforms started, ML jobs/datafeeds opened, CCR follower indices, point-in-time and
     * async-search handles, etc.). Unlike {@link #WRITE_OCCURRED}, this flag is NOT reset
     * between YAML setup and body, so a setup-phase {@code _start} or {@code _open} still
     * forces end-of-test teardown even if the body itself is read-only. Reset per test.
     */
    private static final AtomicBoolean PERSISTENT_RESOURCE_CREATED = new AtomicBoolean(false);

    private static volatile Predicate<Request> readPredicate = LazyRefreshRestClient::defaultIsReadRequest;

    private static volatile Predicate<Request> persistentResourcePredicate = LazyRefreshRestClient::defaultIsPersistentResourceRequest;

    private static final Set<String> DEFAULT_READ_PATH_SUFFIXES = Set.of(
        "_search",
        "_msearch",
        "_count",
        "_terms_enum",
        "_field_caps",
        "_field_usage_stats",
        "_validate/query",
        "_render/template",
        "_search_shards",
        "_explain",
        "_pit",
        "_async_search"
    );

    /**
     * Path suffixes whose invocation creates a persistent or long-running resource on the
     * cluster (background task, persistent task assignment, or stateful handle) that the test
     * framework must clean up before the next test. Membership here is independent of
     * {@link #DEFAULT_READ_PATH_SUFFIXES}: e.g. {@code _pit} and {@code _async_search} are
     * "reads" in HTTP semantics but still leave behind handles that need cleanup.
     */
    private static final Set<String> DEFAULT_PERSISTENT_RESOURCE_PATH_SUFFIXES = Set.of(
        "_start",
        "_open",
        "_follow",
        "_resume_follow",
        "_pit",
        "_async_search"
    );

    private final RestClient delegate;
    private boolean constructed;

    public LazyRefreshRestClient(RestClient delegate) {
        // Pass minimal dummy args to the package-private RestClient constructor; we override every
        // public method to delegate, so the parent state is never observed by callers. The parent's
        // constructor calls setNodes() via virtual dispatch — our override falls back to super for
        // that single bootstrap call (see #setNodes).
        super(
            HttpAsyncClients.createDefault(),
            new Header[0],
            List.of(new Node(new HttpHost("localhost", 9200))),
            null,
            new FailureListener(),
            null,
            false,
            false,
            false
        );
        this.delegate = delegate;
        this.constructed = true;
    }

    /** Returns whether any non-read request has been issued since the last reset. */
    public static boolean writeOccurred() {
        return WRITE_OCCURRED.get();
    }

    /** Clears the write-occurred flag. Tests call this between phases (e.g. after YAML setup,
     *  before the body) to scope the flag to the next phase. */
    public static void resetWriteOccurred() {
        WRITE_OCCURRED.set(false);
    }

    /** Returns whether any request creating a persistent cluster-side resource has been issued
     *  since the last reset. */
    public static boolean persistentResourceCreated() {
        return PERSISTENT_RESOURCE_CREATED.get();
    }

    /** Clears the persistent-resource flag. Callers reset this once per test (covering both
     *  setup and body) so end-of-test cleanup decisions reflect this test's state. */
    public static void resetPersistentResourceCreated() {
        PERSISTENT_RESOURCE_CREATED.set(false);
    }

    /**
     * Override the read/write classifier. Pass {@code null} to revert to the default heuristic
     * (HTTP method GET/HEAD/OPTIONS, plus a small set of POST read-paths like {@code _search}).
     */
    public static void setReadRequestPredicate(Predicate<Request> predicate) {
        readPredicate = predicate != null ? predicate : LazyRefreshRestClient::defaultIsReadRequest;
    }

    /**
     * Override the persistent-resource classifier. Pass {@code null} to revert to the default
     * suffix list ({@code _start}, {@code _open}, {@code _follow}, {@code _resume_follow},
     * {@code _pit}, {@code _async_search}).
     */
    public static void setPersistentResourceRequestPredicate(Predicate<Request> predicate) {
        persistentResourcePredicate = predicate != null ? predicate : LazyRefreshRestClient::defaultIsPersistentResourceRequest;
    }

    private static boolean defaultIsReadRequest(Request request) {
        String method = request.getMethod();
        if ("GET".equals(method) || "HEAD".equals(method) || "OPTIONS".equals(method)) {
            return true;
        }
        if ("POST".equals(method)) {
            String endpoint = request.getEndpoint();
            int q = endpoint.indexOf('?');
            String path = q >= 0 ? endpoint.substring(0, q) : endpoint;
            for (String suffix : DEFAULT_READ_PATH_SUFFIXES) {
                if (path.endsWith("/" + suffix) || path.equals(suffix) || path.equals("/" + suffix)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean defaultIsPersistentResourceRequest(Request request) {
        String method = request.getMethod();
        // Only POST/PUT can create resources; GETs that read state never qualify.
        if ("POST".equals(method) == false && "PUT".equals(method) == false) {
            return false;
        }
        String endpoint = request.getEndpoint();
        int q = endpoint.indexOf('?');
        String path = q >= 0 ? endpoint.substring(0, q) : endpoint;
        for (String suffix : DEFAULT_PERSISTENT_RESOURCE_PATH_SUFFIXES) {
            if (path.endsWith("/" + suffix) || path.equals(suffix) || path.equals("/" + suffix)) {
                return true;
            }
        }
        return false;
    }

    private void recordIfWrite(Request request) {
        if (readPredicate.test(request) == false) {
            WRITE_OCCURRED.set(true);
        }
        if (persistentResourcePredicate.test(request)) {
            PERSISTENT_RESOURCE_CREATED.set(true);
        }
    }

    @Override
    public Response performRequest(Request request) throws IOException {
        recordIfWrite(request);
        return delegate.performRequest(request);
    }

    @Override
    public Cancellable performRequestAsync(Request request, ResponseListener responseListener) {
        recordIfWrite(request);
        return delegate.performRequestAsync(request, responseListener);
    }

    @Override
    public List<Node> getNodes() {
        return delegate.getNodes();
    }

    @Override
    public synchronized void setNodes(Collection<Node> nodes) {
        // The parent's constructor calls setNodes() via virtual dispatch before our delegate field
        // is assigned. Defer to super for that bootstrap call so the parent state initializes.
        if (constructed == false) {
            super.setNodes(nodes);
        } else {
            delegate.setNodes(nodes);
        }
    }

    @Override
    public boolean isRunning() {
        return delegate.isRunning();
    }

    @Override
    public HttpAsyncClient getHttpClient() {
        return delegate.getHttpClient();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
