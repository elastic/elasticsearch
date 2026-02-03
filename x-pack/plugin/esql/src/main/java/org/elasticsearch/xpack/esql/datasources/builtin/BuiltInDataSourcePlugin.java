/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.builtin;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.datasources.format.csv.CsvFormatReader;
import org.elasticsearch.xpack.esql.datasources.http.HttpConfiguration;
import org.elasticsearch.xpack.esql.datasources.http.HttpStorageProvider;
import org.elasticsearch.xpack.esql.datasources.local.LocalStorageProvider;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReaderFactory;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;

import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * Built-in data source plugin that provides basic storage providers and format readers.
 * This plugin is always available and provides:
 * <ul>
 *   <li>HTTP/HTTPS storage provider for reading from web servers</li>
 *   <li>Local file system storage provider for testing and development</li>
 *   <li>CSV format reader for simple delimited text files</li>
 * </ul>
 *
 * <p>These implementations have no heavy external dependencies and are suitable
 * for inclusion in the core ESQL plugin.
 *
 * <p>The executor for HTTP operations is obtained from ES ThreadPool rather than
 * creating standalone threads. This ensures proper lifecycle management and
 * monitoring by Elasticsearch.
 */
public class BuiltInDataSourcePlugin implements DataSourcePlugin {

    private final ExecutorService httpExecutor;

    public BuiltInDataSourcePlugin() {
        this.httpExecutor = new DirectExecutorService();
    }

    public BuiltInDataSourcePlugin(ThreadPool threadPool) {
        this.httpExecutor = threadPool.executor(ThreadPool.Names.GENERIC);
    }

    @Override
    public Map<String, StorageProviderFactory> storageProviders(Settings settings) {
        return Map.of(
            "http",
            s -> new HttpStorageProvider(HttpConfiguration.defaults(), httpExecutor),
            "https",
            s -> new HttpStorageProvider(HttpConfiguration.defaults(), httpExecutor),
            "file",
            s -> new LocalStorageProvider()
        );
    }

    @Override
    public Map<String, FormatReaderFactory> formatReaders(Settings settings) {
        return Map.of("csv", (s, blockFactory) -> new CsvFormatReader(blockFactory));
    }

    public ExecutorService httpExecutor() {
        return httpExecutor;
    }

    private static final class DirectExecutorService extends java.util.concurrent.AbstractExecutorService {
        private volatile boolean shutdown = false;

        @Override
        public void execute(Runnable command) {
            command.run();
        }

        @Override
        public void shutdown() {
            shutdown = true;
        }

        @Override
        public java.util.List<Runnable> shutdownNow() {
            shutdown = true;
            return java.util.Collections.emptyList();
        }

        @Override
        public boolean isShutdown() {
            return shutdown;
        }

        @Override
        public boolean isTerminated() {
            return shutdown;
        }

        @Override
        public boolean awaitTermination(long timeout, java.util.concurrent.TimeUnit unit) {
            return true;
        }
    }
}
