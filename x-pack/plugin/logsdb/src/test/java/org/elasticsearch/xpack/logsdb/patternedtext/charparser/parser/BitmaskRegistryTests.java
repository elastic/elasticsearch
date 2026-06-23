/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.TimestampComponentType;

public class BitmaskRegistryTests extends ESTestCase {

    private BitmaskRegistry<SubTokenType> commonRegistry;
    private SubTokenType type1, type2, type3;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        commonRegistry = new BitmaskRegistry<>();
        type1 = new SubTokenType("Type1", null, new int[] { 4, 8, 16 }, TimestampComponentType.NA);
        type2 = new SubTokenType("Type2", null, new int[] { 32, 64, 128 }, TimestampComponentType.NA);
        type3 = new SubTokenType("Type3", null, new int[] { 256, 512, 1024 }, TimestampComponentType.NA);

        commonRegistry.register(type1);
        commonRegistry.register(type2);
        commonRegistry.register(type3);
        commonRegistry.seal();
    }

    public void testGetBitmask() {
        assertEquals(1, commonRegistry.getBitmask(type1));
        assertEquals(2, commonRegistry.getBitmask(type2));
        assertEquals(4, commonRegistry.getBitmask(type3));
    }

    public void testGetBitIndex() {
        assertEquals(0, commonRegistry.getBitIndex(type1));
        assertEquals(1, commonRegistry.getBitIndex(type2));
        assertEquals(2, commonRegistry.getBitIndex(type3));
    }

    public void testGetTypeByBitIndex() {
        assertEquals(type1, commonRegistry.getTypeByBitIndex(0));
        assertEquals(type2, commonRegistry.getTypeByBitIndex(1));
        assertEquals(type3, commonRegistry.getTypeByBitIndex(2));
    }

    public void testGetLeftmostBitIndex() {
        assertEquals(0, BitmaskRegistry.getLeftmostBitIndex(1));
        assertEquals(1, BitmaskRegistry.getLeftmostBitIndex(2));
        assertEquals(1, BitmaskRegistry.getLeftmostBitIndex(3));
        assertEquals(2, BitmaskRegistry.getLeftmostBitIndex(4));
        assertEquals(2, BitmaskRegistry.getLeftmostBitIndex(5));
        assertEquals(2, BitmaskRegistry.getLeftmostBitIndex(6));
        assertEquals(2, BitmaskRegistry.getLeftmostBitIndex(7));
        assertEquals(3, BitmaskRegistry.getLeftmostBitIndex(8));
    }

    public void testGetHighestPriorityType() {
        assertEquals(type1, commonRegistry.getHighestPriorityType(1));
        assertEquals(type2, commonRegistry.getHighestPriorityType(2));
        assertEquals(type2, commonRegistry.getHighestPriorityType(3));
        assertEquals(type3, commonRegistry.getHighestPriorityType(4));
        assertEquals(type3, commonRegistry.getHighestPriorityType(5));
        assertEquals(type3, commonRegistry.getHighestPriorityType(6));
        assertEquals(type3, commonRegistry.getHighestPriorityType(7));
        assertNull(commonRegistry.getHighestPriorityType(8));
    }

    public void testGetCombinedBitmask() {
        assertEquals(7, commonRegistry.getCombinedBitmask()); // 1 | 2 | 4 = 7
    }

    public void testGetHigherLevelBitmaskByPosition() {
        int result = commonRegistry.getHigherLevelBitmaskByPosition(3, 1); // 3 = 0b11
        assertEquals(72, result); // 8 | 64 = 72

        result = commonRegistry.getHigherLevelBitmaskByPosition(5, 2); // 5 = 0b101
        assertEquals(1040, result); // 16 | 1024 = 1040
    }

    public void testRegisterSingleInstance() {
        BitmaskRegistry<SubTokenType> registry = new BitmaskRegistry<>();
        SubTokenType type = new SubTokenType("Type", null, new int[] { 1, 2, 4 }, TimestampComponentType.NA);
        int bitmask = registry.register(type);
        assertEquals(1, bitmask);
    }

    public void testSealingPreventsRegistration() {
        BitmaskRegistry<SubTokenType> registry = new BitmaskRegistry<>();
        SubTokenType type = new SubTokenType("Type", null, new int[] { 1, 2, 4 }, TimestampComponentType.NA);
        registry.register(type);
        registry.seal();
        SubTokenType newType = new SubTokenType("NewType", null, new int[] { 8, 16, 32 }, TimestampComponentType.NA);
        assertThrows(IllegalStateException.class, () -> registry.register(newType));
    }

    public void testRegisterMaximumInstances() {
        BitmaskRegistry<SubTokenType> registry = new BitmaskRegistry<>();
        for (int i = 0; i < 32; i++) {
            registry.register(new SubTokenType("Type" + i, null, new int[] { 1, 2, 4 }, TimestampComponentType.NA));
        }
        SubTokenType type33 = new SubTokenType("Type33", null, new int[] { 8, 16, 32 }, TimestampComponentType.NA);
        assertThrows(IllegalStateException.class, () -> registry.register(type33));
    }
}
