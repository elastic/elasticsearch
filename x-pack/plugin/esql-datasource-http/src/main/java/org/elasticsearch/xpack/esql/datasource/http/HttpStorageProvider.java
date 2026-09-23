/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.http;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.xpack.esql.datasources.StorageIterator;
import org.elasticsearch.xpack.esql.datasources.spi.StorageChildren;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.IOException;
import java.net.http.HttpClient;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * StorageProvider implementation for HTTP/HTTPS using Java's built-in HttpClient.
 *
 * Features:
 * - Full object reads via GET
 * - Range reads via HTTP Range header
 * - Metadata retrieval via HEAD
 * - Configurable timeouts and redirects
 *
 * Note: HTTP/HTTPS does not support directory listing, so listObjects() returns null.
 */
public final class HttpStorageProvider implements StorageProvider {
    private final HttpClient httpClient;
    private final HttpConfiguration config;
    /**
     * One daemon thread for body idle-timeout watchdogs. JDK {@code HttpClient} has no SO_TIMEOUT on
     * streaming GETs; delayed close of the body stream is how we abort a silent connection. Cancelled
     * via {@code FutureUtils.cancel} (no interrupt).
     */
    private final ScheduledThreadPoolExecutor idleScheduler;

    /**
     * Creates an HttpStorageProvider with configuration and executor.
     *
     * @param config the HTTP configuration
     * @param executor the executor service for async operations
     */
    public HttpStorageProvider(HttpConfiguration config, ExecutorService executor) {
        if (config == null) {
            throw new IllegalArgumentException("config cannot be null");
        }
        if (executor == null) {
            throw new IllegalArgumentException("executor cannot be null");
        }

        this.config = config;
        this.httpClient = HttpClient.newBuilder()
            .connectTimeout(config.connectTimeout())
            .followRedirects(config.followRedirects() ? HttpClient.Redirect.NORMAL : HttpClient.Redirect.NEVER)
            .executor(executor)
            .build();
        ScheduledThreadPoolExecutor scheduler = new Scheduler.SafeScheduledThreadPoolExecutor(
            1,
            EsExecutors.daemonThreadFactory(Settings.EMPTY, "esql-http-idle")
        );
        scheduler.setRemoveOnCancelPolicy(true);
        scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        this.idleScheduler = scheduler;
    }

    @Override
    public StorageObject newObject(StoragePath path) {
        validateHttpScheme(path);
        return new HttpStorageObject(httpClient, path, config, idleScheduler);
    }

    @Override
    public StorageObject newObject(StoragePath path, long length) {
        validateHttpScheme(path);
        return new HttpStorageObject(httpClient, path, config, length, idleScheduler);
    }

    @Override
    public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
        validateHttpScheme(path);
        return new HttpStorageObject(httpClient, path, config, length, lastModified, idleScheduler);
    }

    @Override
    public StorageIterator listObjects(StoragePath prefix, boolean recursive) throws IOException {
        throw new UnsupportedOperationException("HTTP does not support directory listing");
    }

    @Override
    public StorageChildren listChildren(StoragePath prefix, int limit) {
        return null; // HTTP has no directory listing at all — callers fall back to explicit paths
    }

    @Override
    public boolean exists(StoragePath path) throws IOException {
        validateHttpScheme(path);
        StorageObject object = newObject(path);
        return object.exists();
    }

    @Override
    public List<String> supportedSchemes() {
        return List.of("http", "https");
    }

    @Override
    public boolean supportsStableMetadata() {
        return false;
    }

    @Override
    public void close() {
        Scheduler.terminate(idleScheduler, 5, TimeUnit.SECONDS);
        // HttpClient implements AutoCloseable in Java 21+
        // Closing it shuts down the internal selector thread and connection pool
        httpClient.close();
    }

    private void validateHttpScheme(StoragePath path) {
        String scheme = path.scheme().toLowerCase(Locale.ROOT);
        if ("http".equals(scheme) == false && "https".equals(scheme) == false) {
            throw new IllegalArgumentException("HttpStorageProvider only supports http:// and https:// schemes, got: " + scheme);
        }
    }

    public HttpClient httpClient() {
        return httpClient;
    }

    public HttpConfiguration config() {
        return config;
    }

    @Override
    public String toString() {
        return "HttpStorageProvider{config=" + config + "}";
    }
}
