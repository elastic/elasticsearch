/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.seektracker;

import java.util.HashMap;
import java.util.Map;

public class SeekStatsService {

    private final Map<String, IndexSeekTracker> seeks = new HashMap<>();

    public IndexSeekTracker registerIndex(String index) {
        return seeks.computeIfAbsent(index, IndexSeekTracker::new);
    }

    public Map<String, IndexSeekTracker> getSeekStats() {
        return seeks;
    }

    public IndexSeekTracker getSeekStats(String index) {
        return seeks.get(index);
    }

}
