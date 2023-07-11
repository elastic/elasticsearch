/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.lucene.DataPartitioning;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Objects;

/**
 * Holds the pragmas for an ESQL query. Just a wrapper of settings for now.
 */
public final class QueryPragmas implements Writeable {
    public static final int DEFAULT_PAGE_SIZE = 16 * 1024;
    public static final Setting<Integer> EXCHANGE_BUFFER_SIZE = Setting.intSetting("exchange_buffer_size", 10);
    public static final Setting<Integer> EXCHANGE_CONCURRENT_CLIENTS = Setting.intSetting("exchange_concurrent_clients", 3);
    private static final Setting<Integer> TASK_CONCURRENCY = Setting.intSetting(
        "task_concurrency",
        ThreadPool.searchOrGetThreadPoolSize(EsExecutors.allocatedProcessors(Settings.EMPTY))
    );

    public static final Setting<DataPartitioning> DATA_PARTITIONING = Setting.enumSetting(
        DataPartitioning.class,
        "data_partitioning",
        DataPartitioning.SEGMENT
    );

    public static final Setting<Integer> PAGE_SIZE = Setting.intSetting("page_size", DEFAULT_PAGE_SIZE, 1);

    public static final QueryPragmas EMPTY = new QueryPragmas(Settings.EMPTY);

    private final Settings settings;

    public QueryPragmas(Settings settings) {
        this.settings = settings;
    }

    public QueryPragmas(StreamInput in) throws IOException {
        this.settings = Settings.readSettingsFromStream(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        settings.writeTo(out);
    }

    public int exchangeBufferSize() {
        return EXCHANGE_BUFFER_SIZE.get(settings);
    }

    public int concurrentExchangeClients() {
        return EXCHANGE_CONCURRENT_CLIENTS.get(settings);
    }

    public DataPartitioning dataPartitioning() {
        return DATA_PARTITIONING.get(settings);
    }

    public int taskConcurrency() {
        return TASK_CONCURRENCY.get(settings);
    }

    public int pageSize() {
        return PAGE_SIZE.get(settings);
    }

    public boolean isEmpty() {
        return settings.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryPragmas pragmas = (QueryPragmas) o;
        return settings.equals(pragmas.settings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(settings);
    }
}
