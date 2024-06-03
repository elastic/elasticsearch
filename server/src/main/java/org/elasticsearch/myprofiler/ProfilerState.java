/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.myprofiler;

import java.util.HashMap;
import java.util.Map;

public class ProfilerState {
    private static ProfilerState instance;
    private boolean profiling;
    private int queryCount;
    private Map<String,Integer> index_query_count;

    private ProfilerState() {
        this.profiling = false;
        this.queryCount = 0;
        this.index_query_count = new HashMap<>();
    }

    public static synchronized ProfilerState getInstance() {
        if (instance == null) {
            instance = new ProfilerState();
        }
        return instance;
    }

    public synchronized void enableProfiling() {
        this.profiling = true;
    }

    public synchronized void disableProfiling() {
        this.profiling = false;
    }

    public synchronized void incrementQueryCount() {
        if (profiling) {
            queryCount++;
        }
    }

    public synchronized int getStatus(){
        if(profiling){
            return 1;
        }
        else return 0;
    }

    public synchronized int getQueryCount() {
        return queryCount;
    }

    public synchronized void resetQueryCount() {
        queryCount = 0;
    }
    public synchronized Map<String,Integer> getIndex_query_count(){
        return index_query_count;
    }
}
