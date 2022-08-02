/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search;

import org.elasticsearch.Version;
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
        super(in);
        this.canMatch = in.readBoolean();
        if (in.getVersion().onOrAfter(Version.V_7_6_0)) {
            estimatedMinAndMax = in.readOptionalWriteable(MinAndMax::new);
        } else {
            estimatedMinAndMax = null;
        }
    }

    public CanMatchShardResponse(boolean canMatch, MinAndMax<?> estimatedMinAndMax) {
        this.canMatch = canMatch;
        this.estimatedMinAndMax = estimatedMinAndMax;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(canMatch);
        if (out.getVersion().onOrAfter(Version.V_7_6_0)) {
            out.writeOptionalWriteable(estimatedMinAndMax);
        }
    }

    public boolean canMatch() {
        return canMatch;
    }

    public MinAndMax<?> estimatedMinAndMax() {
        return estimatedMinAndMax;
    }
}
