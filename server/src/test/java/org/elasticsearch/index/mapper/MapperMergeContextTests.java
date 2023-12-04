/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;

public class MapperMergeContextTests extends ESTestCase {

    public void testAddFieldIfPossibleUnderLimit() {
        MapperMergeContext context = MapperMergeContext.root(false, false, 1);
        assertTrue(context.canAddField(1));
        HashMap<String, Mapper> mappers = new HashMap<>();
        context.addFieldIfPossible(mappers, getKeywordFieldMapper());
        assertEquals(1, mappers.size());
        assertFalse(context.canAddField(1));
    }

    public void testAddFieldIfPossibleAtLimit() {
        MapperMergeContext context = MapperMergeContext.root(false, false, 0);
        assertFalse(context.canAddField(1));
        HashMap<String, Mapper> mappers = new HashMap<>();
        context.addFieldIfPossible(mappers, getKeywordFieldMapper());
        assertEquals(0, mappers.size());
        assertFalse(context.canAddField(1));
    }

    public void testAddRuntimeFieldIfPossibleUnderLimit() {
        MapperMergeContext context = MapperMergeContext.root(false, false, 1);
        assertTrue(context.canAddField(1));
        HashMap<String, RuntimeField> runtimeFields = new HashMap<>();
        context.addRuntimeFieldIfPossible(runtimeFields, new TestRuntimeField("foo", "keyword"));
        assertEquals(1, runtimeFields.size());
        assertFalse(context.canAddField(1));
    }

    public void testAddRuntimeFieldIfPossibleAtLimit() {
        MapperMergeContext context = MapperMergeContext.root(false, false, 0);
        assertFalse(context.canAddField(1));
        HashMap<String, RuntimeField> runtimeFields = new HashMap<>();
        context.addRuntimeFieldIfPossible(runtimeFields, new TestRuntimeField("foo", "keyword"));
        assertEquals(0, runtimeFields.size());
        assertFalse(context.canAddField(1));
    }

    public void testRemoveRuntimeField() {
        MapperMergeContext context = MapperMergeContext.root(false, false, 1);
        HashMap<String, RuntimeField> runtimeFields = new HashMap<>();
        context.addRuntimeFieldIfPossible(runtimeFields, new TestRuntimeField("foo", "keyword"));
        assertEquals(1, runtimeFields.size());
        assertFalse(context.canAddField(1));

        context.removeRuntimeField(runtimeFields, "foo");
        assertTrue(context.canAddField(1));
    }

    private static KeywordFieldMapper getKeywordFieldMapper() {
        return new KeywordFieldMapper.Builder("foo", IndexVersion.current()).build(MapperBuilderContext.root(false, false));
    }

}
