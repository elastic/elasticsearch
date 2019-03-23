/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.transforms;

import org.elasticsearch.test.ESTestCase;

public class IDGeneratorTests extends ESTestCase {

    public void testSupportedTypes() {
        IDGenerator idGen = new IDGenerator();
        idGen.add("key1");
        String id = idGen.getID();
        idGen.add(null);
        assertNotEquals(id, idGen.getID());
        id = idGen.getID();
        idGen.add("key2");
        assertNotEquals(id, idGen.getID());
        id = idGen.getID();
        idGen.add(12L);
        assertNotEquals(id, idGen.getID());
        id = idGen.getID();
        idGen.add(44.444);
        assertNotEquals(id, idGen.getID());
    }

    public void testOrderDependence() {
        IDGenerator idGen = new IDGenerator();
        idGen.add("key1");
        idGen.add("key2");
        String id1 = idGen.getID();

        idGen = new IDGenerator();
        idGen.add("key2");
        idGen.add("key1");
        String id2 = idGen.getID();

        assertNotEquals(id1, id2);
    }

    public void testEmptyThrows() {
        IDGenerator idGen = new IDGenerator();

        RuntimeException e = expectThrows(RuntimeException.class, () -> idGen.getID());

        assertEquals("Add at least 1 object before generating the ID", e.getMessage());
    }

}
