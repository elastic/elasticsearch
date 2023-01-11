/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.engine;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

/*
 * Captures the result of an {@link org.elasticsearch.index.engine.Engine#refresh(String)} call.
 */
public class RefreshResult implements Writeable {

    public static final long UNKNOWN_SEGMENT_GEN = -1L;

    private final boolean refreshed;
    private final long segmentGeneration;

    public static final RefreshResult EMPTY = new RefreshResult(false);

    public RefreshResult(boolean refreshed) {
        this(refreshed, UNKNOWN_SEGMENT_GEN);
    }

    public RefreshResult(boolean refreshed, long segmentGeneration) {
        this.refreshed = refreshed;
        this.segmentGeneration = segmentGeneration;
    }

    public RefreshResult(StreamInput in) throws IOException {
        this.refreshed = in.readBoolean();
        this.segmentGeneration = in.readLong();
    }

    public boolean isRefreshed() {
        return refreshed;
    }

    public long getSegmentGeneration() {
        return segmentGeneration;
    }

    public boolean isSegmentGenerationPresent() {
        return segmentGeneration != UNKNOWN_SEGMENT_GEN;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(refreshed);
        out.writeLong(segmentGeneration);
    }
}
