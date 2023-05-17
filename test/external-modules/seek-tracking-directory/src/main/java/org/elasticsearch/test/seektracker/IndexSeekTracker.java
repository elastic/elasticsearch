/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.seektracker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

public class IndexSeekTracker {

    private final String index;
    private final Map<String, Map<String, LongAdder>> seeks = new HashMap<>();

    public IndexSeekTracker(String index) {
        this.index = index;
    }

    public void track(String shard) {
        seeks.computeIfAbsent(shard, k -> new ConcurrentHashMap<>());   // increment can be called by multiple threads
    }

    public void increment(String shard, String file) {
        seeks.get(shard).computeIfAbsent(file, s -> new LongAdder()).increment();
    }

    public List<ShardSeekStats> getSeeks() {
        List<ShardSeekStats> values = new ArrayList<>();
        seeks.forEach((k, v) -> values.add(getSeeksForShard(k)));
        return values;
    }

    private ShardSeekStats getSeeksForShard(String shard) {
        Map<String, Long> seeksPerFile = new HashMap<>();
        seeks.get(shard).forEach((name, adder) -> seeksPerFile.put(name, adder.longValue()));
        return new ShardSeekStats(shard, seeksPerFile);
    }

    @Override
    public String toString() {
        return "seeks for " + index + ": " + seeks;
    }
}
