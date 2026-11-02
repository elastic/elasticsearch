/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.test.ESTestCase;

import java.util.Set;
import java.util.function.Consumer;

import static org.elasticsearch.index.mapper.MapperService.MergeReason.MAPPING_UPDATE;

public class MapperMergeContextTests extends ESTestCase {

    public void testAddFieldIfPossibleUnderLimit() {
        MapperMergeContext context = new TestRootMapperMergeContextBuilder(MAPPING_UPDATE).newFieldsBudget(1).build();
        assertTrue(context.decrementFieldBudgetIfPossible(1));
        assertFalse(context.decrementFieldBudgetIfPossible(1));
    }

    public void testAddFieldIfPossibleAtLimit() {
        MapperMergeContext context = new TestRootMapperMergeContextBuilder(MAPPING_UPDATE).newFieldsBudget(0).build();
        assertFalse(context.decrementFieldBudgetIfPossible(1));
    }

    public void testAddFieldIfPossibleUnlimited() {
        MapperMergeContext context = new TestRootMapperMergeContextBuilder(MAPPING_UPDATE).build();
        assertTrue(context.decrementFieldBudgetIfPossible(Integer.MAX_VALUE));
        assertTrue(context.decrementFieldBudgetIfPossible(Integer.MAX_VALUE));
    }

    public void testMergeReasons() {
        MapperService.MergeReason mergeReason = randomFrom(MapperService.MergeReason.values());
        MapperMergeContext context = new TestRootMapperMergeContextBuilder(mergeReason).build();
        assertEquals(mergeReason, context.getMapperBuilderContext().getMergeReason());
    }

    public void testClusterHasFeatureUsesPredicate() {
        final NodeFeature FEATURE_A = new NodeFeature("test.feature_a");
        final NodeFeature FEATURE_B = new NodeFeature("test.feature_b");
        final Set<NodeFeature> supportedFeatures = Set.of(FEATURE_A);
        final Consumer<MapperMergeContext> assertFeatures = c -> {
            assertTrue(c.clusterHasFeature(FEATURE_A));
            assertFalse(c.clusterHasFeature(FEATURE_B));
        };

        MapperMergeContext context = new TestRootMapperMergeContextBuilder(MAPPING_UPDATE).clusterSupportsFeature(
            supportedFeatures::contains
        ).build();
        assertFeatures.accept(context);
        assertFeatures.accept(context.createChildContext("child", null));
        assertFeatures.accept(context.withUnlimitedParseFieldLimitsBudget());

        MapperBuilderContext otherBuilderContext = MapperBuilderContext.root(false, false);
        MapperMergeContext withOtherBuilderContext = context.withUnlimitedParseFieldLimitsBudget(otherBuilderContext);
        assertFeatures.accept(withOtherBuilderContext);
        assertSame(otherBuilderContext, withOtherBuilderContext.getMapperBuilderContext());
    }
}
