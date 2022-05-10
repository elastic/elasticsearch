/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging;

import org.elasticsearch.test.ESTestCase;

public class LevelTests extends ESTestCase {

    public void testEquality() {
        Level lvl1 = Level.forName("lvl1", 1);
        Level lvl1Too = Level.forName("lvl1", 1);
        assertTrue(lvl1 == lvl1Too);
        assertTrue(lvl1.equals(lvl1Too));
        assertTrue(lvl1Too.equals(lvl1));
    }

    public void testCreationMethods() {
        Level lvl1 = Level.forName("lvl1", 1);
        Level lvl1Too = Level.forName("lvl1");
        Level lvl1Upper = Level.forName("LVL1");
        assertTrue(lvl1 == lvl1Too);
        assertTrue(lvl1Too == lvl1Upper);
    }

    public void testNameNotNull() {
        expectThrows(NullPointerException.class, () -> Level.forName(null, 1));
        expectThrows(NullPointerException.class, () -> Level.forName(null));
    }
}
