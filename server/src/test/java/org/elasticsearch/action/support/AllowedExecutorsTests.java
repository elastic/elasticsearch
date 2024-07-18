/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.elasticsearch.test.ESTestCase;

import java.util.Map;
import java.util.Set;

import static org.elasticsearch.action.support.AllowedExecutors.getAllowedExecutors;
import static org.elasticsearch.action.support.AllowedExecutors.newEntry;

public class AllowedExecutorsTests extends ESTestCase {
    public void test() {
        assertEquals(Map.of(), getAllowedExecutors());
        assertEquals(Map.of("p1", Set.of()), getAllowedExecutors(newEntry("p1")));
        assertEquals(Map.of("p1", Set.of(), "p2", Set.of()), getAllowedExecutors(newEntry("p1"), newEntry("p2")));
        assertEquals(Map.of("p1", Set.of(), "p2", Set.of("p1")), getAllowedExecutors(newEntry("p1"), newEntry("p2", "p1")));
        expectThrows(IllegalStateException.class, () -> getAllowedExecutors(newEntry("p1", "p2")));
        expectThrows(IllegalStateException.class, () -> getAllowedExecutors(newEntry("p2", "p1"), newEntry("p1")));
        expectThrows(IllegalStateException.class, () -> getAllowedExecutors(newEntry("p1"), newEntry("p2", "p2")));
        expectThrows(IllegalStateException.class, () -> getAllowedExecutors(newEntry("p1"), newEntry("p1")));
        AllowedExecutors.getProductionAllowedExecutors(); // must not throw
    }
}
