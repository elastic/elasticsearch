/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.profile.query;

import org.elasticsearch.search.profile.ProfileResult;

public class DfsProfiler {

    private final DfsProfileBreakdown dfsProfileBreakdown;

    public DfsProfiler() {
        dfsProfileBreakdown = new DfsProfileBreakdown();
    }

    public void startTotal() {
        dfsProfileBreakdown.start(System.nanoTime());
    }

    public void stopTotal() {
        dfsProfileBreakdown.stop(System.nanoTime());
    }

    public void startTiming(DfsTimingType type) {
        dfsProfileBreakdown.getTimer(type).start();
    }

    public void stopTiming(DfsTimingType type) {
        dfsProfileBreakdown.getTimer(type).stop();
    }

    public ProfileResult result() {
        return dfsProfileBreakdown.result();
    }
}
