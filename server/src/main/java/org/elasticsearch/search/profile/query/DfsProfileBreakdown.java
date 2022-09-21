/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.profile.query;

import org.elasticsearch.search.profile.AbstractProfileBreakdown;
import org.elasticsearch.search.profile.ProfileResult;

import java.util.List;
import java.util.Map;

/**
 * A record of timings for the various operations that may happen during dfs execution.
 */
public final class DfsProfileBreakdown extends AbstractProfileBreakdown<DfsTimingType> {

    private long start;
    private long stop;

    public DfsProfileBreakdown() {
        super(DfsTimingType.class);
    }

    public void start(long start) {
        this.start = start;
    }

    public void stop(long stop) {
        this.stop = stop;
    }

    ProfileResult result() {
        return new ProfileResult("dfs", "dfs phase", toBreakdownMap(), toDebugMap(), stop - start,
            List.of(new ProfileResult("inner", "test", Map.of(), Map.of(), 0, null)));
    }
}
