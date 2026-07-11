/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Factory for a bounded, thread-safe "warn-once" map keyed by (sanitized) URL. Backs the once-per-endpoint WARN
 * de-duplication in {@link JdbcConnector} (unsupported-version and product-name advisories) and {@link JdbcHikariPool}
 * (pool keepalive/validation clamp warnings) so a node that reaches an unbounded number of distinct JDBC endpoints
 * cannot grow the guard maps without limit.
 * <p>
 * The returned map keeps at most {@code maxEntries} keys; inserting past the cap evicts the eldest key in
 * insertion order (FIFO). Eviction is behaviourally harmless for a warn-once guard: the only consequence of
 * evicting a long-idle endpoint's key is that its one-time WARN may fire once more if that endpoint is reached
 * again later. The "warn once per URL (until evicted)" contract is preserved via the usual
 * {@code putIfAbsent(key, TRUE) == null} idiom, which triggers eviction atomically under the map's lock.
 * <p>
 * The backing {@link LinkedHashMap} is wrapped in {@link Collections#synchronizedMap} because JDBC producers run
 * concurrently on {@code esql_worker} threads; {@code putIfAbsent}/{@code remove}/{@code containsKey} are all
 * synchronized on the wrapper's mutex.
 */
final class BoundedWarnOnceMap {

    /**
     * Default cap on the number of distinct keys retained before the eldest is evicted. Sized generously relative
     * to the number of distinct JDBC endpoints a node realistically talks to, so eviction is effectively never hit
     * in normal operation and only guards against pathological endpoint churn.
     */
    static final int DEFAULT_MAX_ENTRIES = 1024;

    private BoundedWarnOnceMap() {}

    /** A bounded warn-once map with the {@link #DEFAULT_MAX_ENTRIES default} cap. */
    static Map<String, Boolean> create() {
        return create(DEFAULT_MAX_ENTRIES);
    }

    /**
     * A bounded warn-once map holding at most {@code maxEntries} keys; the eldest (insertion-order) key is evicted
     * when a new insertion would exceed the cap.
     */
    static Map<String, Boolean> create(int maxEntries) {
        if (maxEntries < 1) {
            throw new IllegalArgumentException("maxEntries must be >= 1");
        }
        return Collections.synchronizedMap(new LinkedHashMap<>(16, 0.75f, false) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                return size() > maxEntries;
            }
        });
    }
}
