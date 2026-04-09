/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

/**
 * Specifies whether reroute() should be called after applying request.
 */
public enum RerouteBehavior {
    /** Invoke reroute after applying the request. */
    PERFORM_REROUTE,
    /** Do not reroute; the caller is responsible for ensuring that a follow-up reroute occurs. */
    SKIP_REROUTE;

    /**
     * Returns {@link #PERFORM_REROUTE} if either {@code this} or {@code other} is {@link #PERFORM_REROUTE}.
     */
    public RerouteBehavior or(RerouteBehavior other) {
        if (this == PERFORM_REROUTE || other == PERFORM_REROUTE) {
            return PERFORM_REROUTE;
        }
        return SKIP_REROUTE;
    }
}
