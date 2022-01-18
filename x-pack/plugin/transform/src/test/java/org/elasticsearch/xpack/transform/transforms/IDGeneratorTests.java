/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.elasticsearch.test.ESTestCase;

public class IDGeneratorTests extends ESTestCase {

    public void testSupportedTypes() {
        IDGenerator idGen = new IDGenerator();
        idGen.add("key1", "value1");
        String id = idGen.getID();
        idGen.add("key2", null);
        assertNotEquals(id, idGen.getID());
        id = idGen.getID();
        idGen.add("key3", "value3");
        assertNotEquals(id, idGen.getID());
        id = idGen.getID();
        idGen.add("key4", 12L);
        assertNotEquals(id, idGen.getID());
        id = idGen.getID();
        idGen.add("key5", 44.444);
        assertNotEquals(id, idGen.getID());
        idGen.add("key6", 13);
        assertNotEquals(id, idGen.getID());
        id = idGen.getID();
        idGen.add("key7", "");
        assertNotEquals(id, idGen.getID());
        idGen.add("key8", true);
        assertNotEquals(id, idGen.getID());
    }

    public void testOrderIndependence() {
        IDGenerator idGen = new IDGenerator();
        idGen.add("key1", "value1");
        idGen.add("key2", "value2");
        String id1 = idGen.getID();

        idGen = new IDGenerator();
        idGen.add("key2", "value2");
        idGen.add("key1", "value1");
        String id2 = idGen.getID();

        assertEquals(id1, id2);
    }

    public void testEmptyThrows() {
        IDGenerator idGen = new IDGenerator();

        RuntimeException e = expectThrows(RuntimeException.class, () -> idGen.getID());

        assertEquals("Add at least 1 object before generating the ID", e.getMessage());
    }

    public void testDuplicatedKeyThrows() {
        IDGenerator idGen = new IDGenerator();
        idGen.add("key1", "value1");

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> idGen.add("key1", "some_other_value"));

        assertEquals("Keys must be unique", e.getMessage());
    }

}
