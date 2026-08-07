/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc;

import org.elasticsearch.test.ESTestCase;

import java.util.Map;

/**
 * Unit tests for {@link BoundedWarnOnceMap}: the warn-once ({@code putIfAbsent}) contract and the bounded FIFO
 * eviction that keeps the {@link JdbcConnector} / {@link JdbcHikariPool} WARN-dedupe guards from growing without
 * limit as a node reaches ever more distinct JDBC endpoints.
 */
public class BoundedWarnOnceMapTests extends ESTestCase {

    public void testWarnOnceContract() {
        Map<String, Boolean> map = BoundedWarnOnceMap.create();
        assertNull("first sighting must be absent", map.putIfAbsent("jdbc:postgresql://host/db", Boolean.TRUE));
        assertEquals(
            "second sighting must be present (warn suppressed)",
            Boolean.TRUE,
            map.putIfAbsent("jdbc:postgresql://host/db", Boolean.TRUE)
        );
        assertTrue(map.containsKey("jdbc:postgresql://host/db"));
    }

    public void testEvictsEldestBeyondCap() {
        Map<String, Boolean> map = BoundedWarnOnceMap.create(3);
        for (int i = 0; i < 3; i++) {
            assertNull(map.putIfAbsent("url-" + i, Boolean.TRUE));
        }
        // Inserting a 4th key evicts the eldest (url-0) so the map never exceeds the cap.
        assertNull(map.putIfAbsent("url-3", Boolean.TRUE));
        assertEquals("map must not exceed its cap", 3, map.size());
        assertFalse("eldest key must have been evicted", map.containsKey("url-0"));
        assertTrue(map.containsKey("url-3"));
        // Because url-0 was evicted, its warn-once token is gone: seeing it again is treated as a first sighting.
        assertNull("evicted key is a fresh first-sighting again", map.putIfAbsent("url-0", Boolean.TRUE));
    }

    public void testRemoveClearsToken() {
        Map<String, Boolean> map = BoundedWarnOnceMap.create();
        map.putIfAbsent("url", Boolean.TRUE);
        assertTrue(map.containsKey("url"));
        map.remove("url");
        assertFalse(map.containsKey("url"));
        assertNull("removed key warns again", map.putIfAbsent("url", Boolean.TRUE));
    }

    public void testRejectsNonPositiveCap() {
        expectThrows(IllegalArgumentException.class, () -> BoundedWarnOnceMap.create(0));
        expectThrows(IllegalArgumentException.class, () -> BoundedWarnOnceMap.create(-1));
    }
}
