/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.sort.MinAndMax;

import java.io.IOException;

/**
 * Shard-level response for can-match requests
 */
public final class CanMatchShardResponse extends SearchPhaseResult {
    private final boolean canMatch;
    private final MinAndMax<?> estimatedMinAndMax;

    public CanMatchShardResponse(StreamInput in) throws IOException {
        this.canMatch = in.readBoolean();
        estimatedMinAndMax = in.readOptionalWriteable(MinAndMax::new);
    }

    public CanMatchShardResponse(boolean canMatch, MinAndMax<?> estimatedMinAndMax) {
        this.canMatch = canMatch;
        this.estimatedMinAndMax = estimatedMinAndMax;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(canMatch);
        out.writeOptionalWriteable(estimatedMinAndMax);
    }

    public boolean canMatch() {
        return canMatch;
    }

    public MinAndMax<?> estimatedMinAndMax() {
        return estimatedMinAndMax;
    }
}
