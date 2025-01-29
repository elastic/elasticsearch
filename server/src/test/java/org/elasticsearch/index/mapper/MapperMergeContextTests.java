/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.index.mapper.MapperService.MergeReason.MAPPING_UPDATE;

public class MapperMergeContextTests extends ESTestCase {

    public void testAddFieldIfPossibleUnderLimit() {
        MapperMergeContext context = MapperMergeContext.root(false, false, MAPPING_UPDATE, 1);
        assertTrue(context.decrementFieldBudgetIfPossible(1));
        assertFalse(context.decrementFieldBudgetIfPossible(1));
    }

    public void testAddFieldIfPossibleAtLimit() {
        MapperMergeContext context = MapperMergeContext.root(false, false, MAPPING_UPDATE, 0);
        assertFalse(context.decrementFieldBudgetIfPossible(1));
    }

    public void testAddFieldIfPossibleUnlimited() {
        MapperMergeContext context = MapperMergeContext.root(false, false, MAPPING_UPDATE, Long.MAX_VALUE);
        assertTrue(context.decrementFieldBudgetIfPossible(Integer.MAX_VALUE));
        assertTrue(context.decrementFieldBudgetIfPossible(Integer.MAX_VALUE));
    }

    public void testMergeReasons() {
        MapperService.MergeReason mergeReason = randomFrom(MapperService.MergeReason.values());
        MapperMergeContext context = MapperMergeContext.root(false, false, mergeReason, Integer.MAX_VALUE);
        assertEquals(mergeReason, context.getMapperBuilderContext().getMergeReason());
    }

}
