/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.cbo;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.Objects;

/** Per-query inputs shared by statistics derivation and the distribution search. */
public record CboContext(Configuration configuration, TransportVersion minimumVersion, int dataNodes, long materializeLimitBytes) {
    public CboContext {
        Objects.requireNonNull(configuration);
        Objects.requireNonNull(minimumVersion);
        if (dataNodes < 0) {
            throw new IllegalArgumentException("dataNodes must be non-negative");
        }
        if (materializeLimitBytes < 0) {
            throw new IllegalArgumentException("materializeLimitBytes must be non-negative");
        }
    }

    /** Returns at least one worker lane so local plans can use the same cost formulas. */
    public int workerNodes() {
        return Math.max(1, dataNodes);
    }
}
