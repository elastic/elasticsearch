/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health.node;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.List;

/**
 * A snapshot of indices that are stalled in one category of the DLM frozen-tier transition.
 *
 * @param totalCount the total number of stalled indices detected across all projects (may exceed {@code sample.size()}
 *                   when the publisher cap has been reached)
 * @param sample     a capped sample of the stalled indices, ordered by the iteration over projects and data streams;
 *                   always a subset of the total when {@code totalCount > sample.size()}
 */
public record StalledIndices(int totalCount, List<DlmFrozenTransitionIndexInfo> sample) implements Writeable {

    public static final StalledIndices EMPTY = new StalledIndices(0, List.of());

    public StalledIndices {
        sample = List.copyOf(sample);
    }

    public StalledIndices(StreamInput in) throws IOException {
        this(in.readVInt(), in.readCollectionAsList(DlmFrozenTransitionIndexInfo::new));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(totalCount);
        out.writeCollection(sample);
    }

    public boolean isEmpty() {
        return totalCount == 0;
    }
}
