/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.analysis;

public class AnalyzeState {
    private final int lastPosition;
    private final int lastOffset;

    public AnalyzeState(int lastPosition, int lastOffset) {
        this.lastPosition = lastPosition;
        this.lastOffset = lastOffset;
    }

    public AnalyzeState(AnalyzeState prev) {
        this.lastPosition = prev.lastPosition;
        this.lastOffset = prev.lastOffset;
    }

    public int getLastOffset() {
        return lastOffset;
    }

    public int getLastPosition() {
        return lastPosition;
    }
}
