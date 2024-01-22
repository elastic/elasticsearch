/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.MapperMergeContext.NewFieldsBudget;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

public class MapperMergeContextTests extends ESTestCase {

    public void testAddFieldIfPossibleUnderLimit() {
        MapperMergeContext context = MapperMergeContext.root(false, false, NewFieldsBudget.of(1));
        assertTrue(context.hasRemainingBudget());
        HashMap<String, Mapper> mappers = new HashMap<>();
        Mapper mapper = getKeywordFieldMapper();
        context.addFieldIfPossible(mapper, m -> ((Map<String, Mapper>) mappers).put(mapper.simpleName(), mapper));
        assertEquals(1, mappers.size());
        assertFalse(context.hasRemainingBudget());
    }

    public void testAddFieldIfPossibleAtLimit() {
        MapperMergeContext context = MapperMergeContext.root(false, false, NewFieldsBudget.of(0));
        assertFalse(context.hasRemainingBudget());
        HashMap<String, Mapper> mappers = new HashMap<>();
        Mapper mapper = getKeywordFieldMapper();
        context.addFieldIfPossible(mapper, m -> ((Map<String, Mapper>) mappers).put(mapper.simpleName(), mapper));
        assertEquals(0, mappers.size());
        assertFalse(context.hasRemainingBudget());
    }

    public void testAddRuntimeFieldIfPossibleUnderLimit() {
        MapperMergeContext context = MapperMergeContext.root(false, false, NewFieldsBudget.of(1));
        assertTrue(context.hasRemainingBudget());
        HashMap<String, RuntimeField> runtimeFields = new HashMap<>();
        context.addRuntimeFieldIfPossible(new TestRuntimeField("foo", "keyword"), r -> runtimeFields.put(r.name(), r));
        assertEquals(1, runtimeFields.size());
        assertFalse(context.hasRemainingBudget());
    }

    public void testAddRuntimeFieldIfPossibleAtLimit() {
        MapperMergeContext context = MapperMergeContext.root(false, false, NewFieldsBudget.of(0));
        assertFalse(context.hasRemainingBudget());
        HashMap<String, RuntimeField> runtimeFields = new HashMap<>();
        context.addRuntimeFieldIfPossible(new TestRuntimeField("foo", "keyword"), r -> runtimeFields.put(r.name(), r));
        assertEquals(0, runtimeFields.size());
        assertFalse(context.hasRemainingBudget());
    }

    public void testRemoveRuntimeField() {
        MapperMergeContext context = MapperMergeContext.root(false, false, NewFieldsBudget.of(1));
        HashMap<String, RuntimeField> runtimeFields = new HashMap<>();
        context.addRuntimeFieldIfPossible(new TestRuntimeField("foo", "keyword"), r -> runtimeFields.put(r.name(), r));
        assertEquals(1, runtimeFields.size());
        assertFalse(context.hasRemainingBudget());

        context.removeRuntimeField(runtimeFields, "foo");
        assertTrue(context.hasRemainingBudget());
    }

    private static KeywordFieldMapper getKeywordFieldMapper() {
        return new KeywordFieldMapper.Builder("foo", IndexVersion.current()).build(MapperBuilderContext.root(false, false));
    }

}
