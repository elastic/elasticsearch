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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

/**
 * A {@link RestClient} that runs a one-shot, opt-in "refresh" callback on the calling thread
 * before the next non-read request. Tests use {@link #setPendingRefresh(Runnable)} to register
 * deferred work (e.g. a YAML setup section) that should run only when the body actually mutates
 * cluster state. Read-only request paths skip the callback entirely.
 *
 * <p>Unlike an {@code HttpRequestInterceptor} hooked into the async pipeline, the refresh runs
 * synchronously here in {@link #performRequest(Request)} on the test thread, so wipe + setup
 * fully quiesce before the original request is even submitted.</p>
 */
public final class LazyRefreshRestClient extends RestClient {

    private static final AtomicReference<Runnable> PENDING_REFRESH = new AtomicReference<>();
    private static final ThreadLocal<Boolean> IN_REFRESH = ThreadLocal.withInitial(() -> false);
    private static volatile Predicate<Request> readPredicate = LazyRefreshRestClient::defaultIsReadRequest;

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

    /**
     * Register a callback to be run lazily, just before the next non-read HTTP request. The
     * callback is consumed (cleared) once it fires. Pass {@code null} to clear.
     */
    public static void setPendingRefresh(Runnable refresh) {
        PENDING_REFRESH.set(refresh);
    }

    /**
     * Override the read/write classifier. Pass {@code null} to revert to the default heuristic
     * (HTTP method GET/HEAD/OPTIONS, plus a small set of POST read-paths like {@code _search}).
     */
    public static void setReadRequestPredicate(Predicate<Request> predicate) {
        readPredicate = predicate != null ? predicate : LazyRefreshRestClient::defaultIsReadRequest;
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

    private void runPendingRefreshIfNeeded(Request request) {
        if (IN_REFRESH.get()) {
            return;
        }
        if (PENDING_REFRESH.get() == null) {
            return;
        }
        if (readPredicate.test(request)) {
            return;
        }
        Runnable refresh = PENDING_REFRESH.getAndSet(null);
        if (refresh == null) {
            return;
        }
        IN_REFRESH.set(Boolean.TRUE);
        try {
            refresh.run();
        } finally {
            IN_REFRESH.remove();
        }
    }

    @Override
    public Response performRequest(Request request) throws IOException {
        runPendingRefreshIfNeeded(request);
        return delegate.performRequest(request);
    }

    @Override
    public Cancellable performRequestAsync(Request request, ResponseListener responseListener) {
        runPendingRefreshIfNeeded(request);
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
