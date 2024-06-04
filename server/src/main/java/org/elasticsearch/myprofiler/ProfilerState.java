/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.myprofiler;


import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class ProfilerState {
    private static ProfilerState instance;
    private boolean profiling;
    private AtomicLong queryCount;
    private ConcurrentHashMap<String,AtomicLong> index_query_count;

    private ProfilerState() {
        this.profiling = false;
        this.queryCount = new AtomicLong(0);
        this.index_query_count = new ConcurrentHashMap<>();
    }

    public static synchronized ProfilerState getInstance() {
        if (instance == null) {
            instance = new ProfilerState();
        }
        return instance;
    }

    public void enableProfiling() {
        this.profiling = true;
    }

    public void disableProfiling() {
        this.profiling = false;
    }

    public void incrementQueryCount() {
        if (profiling) {
            queryCount.incrementAndGet();
        }
    }

    public synchronized int getStatus(){
        return profiling ? 1:0;
    }

    public long getQueryCount() {
        return queryCount.get();
    }

    public void resetQueryCount() {
        queryCount.set(0);
    }
    public synchronized ConcurrentHashMap<String, AtomicLong> getIndex_query_count(){
        return index_query_count;
    }
}
