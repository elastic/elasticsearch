/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.core.TimeValue;

/**
 * Remembers which node-log warnings were written recently, so one that describes a dataset's condition rather than a
 * query's is written once per window instead of once per query.
 * <p>
 * A warning about how a dataset is laid out holds until someone changes the dataset, and every query over it would
 * otherwise repeat it - a dashboard refreshing every few seconds turns one actionable line into a flood that buries
 * it. Expiring rather than remembering forever is the other half: a condition that is fixed and later recurs is
 * reported again.
 * <p>
 * State lives for as long as the owner does. One per node is the point, so production shares the one its
 * {@link FileSourceFactory} holds; a provider built without one gets its own, which is what keeps a test's warning
 * from being swallowed by another test's.
 */
public final class NodeWarningThrottle {

    static final TimeValue DEFAULT_WINDOW = TimeValue.timeValueHours(1);
    private static final int MAX_KEYS = 1024;

    private final Cache<String, Boolean> written;

    public NodeWarningThrottle() {
        this(DEFAULT_WINDOW);
    }

    NodeWarningThrottle(TimeValue window) {
        this.written = CacheBuilder.<String, Boolean>builder().setMaximumWeight(MAX_KEYS).setExpireAfterWrite(window).build();
    }

    /**
     * Whether the warning named by {@code key} should be written now: {@code true} the first time within the window,
     * {@code false} after. Two threads racing on the same key may both be told {@code true}; one duplicate line is the
     * whole cost, and it is cheaper than a lock on a logging path.
     */
    public boolean firstInWindow(String key) {
        if (written.get(key) != null) {
            return false;
        }
        written.put(key, Boolean.TRUE);
        return true;
    }
}
