/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.shrink;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.action.admin.indices.shrink.ResizeNumberOfShardsCalculator.ShrinkShardsCalculator.calculateAcceptableNumberOfShards;

public class ResizeNumberOfShardsCalculatorTests extends ESTestCase {

    public void testShrink() {
        IndexMetadata indexMetadata = IndexMetadata.builder("index")
            .settings(indexSettings(IndexVersion.current(), randomIntBetween(1, 5), randomIntBetween(0, 5)))
            .build();

        ResizeNumberOfShardsCalculator.ShrinkShardsCalculator shrinkShardsCalculator =
            new ResizeNumberOfShardsCalculator.ShrinkShardsCalculator(
                new StoreStats(between(1, 100), between(0, 100), between(1, 100)),
                (i) -> new DocsStats(between(1, 1000), between(1, 1000), between(0, 10000))
            );
        assertEquals(4, shrinkShardsCalculator.calculate(4, null, indexMetadata));
        assertEquals(1, shrinkShardsCalculator.calculate(null, null, indexMetadata));
        assertEquals(1, shrinkShardsCalculator.calculate(null, ByteSizeValue.ofGb(50), indexMetadata));
        shrinkShardsCalculator.validate(1, indexMetadata);
        expectThrows(IllegalArgumentException.class, () -> shrinkShardsCalculator.calculate(4, ByteSizeValue.ofGb(50), indexMetadata));
        expectThrows(IllegalArgumentException.class, () -> shrinkShardsCalculator.validate(10, indexMetadata));

        assertTrue(
            expectThrows(
                IllegalStateException.class,
                () -> new ResizeNumberOfShardsCalculator.ShrinkShardsCalculator(
                    new StoreStats(between(1, 100), between(0, 100), between(1, 100)),
                    (i) -> new DocsStats(Integer.MAX_VALUE, between(1, 1000), between(1, 100))
                ).validate(1, indexMetadata)
            ).getMessage().startsWith("Can't merge index with more than [2147483519] docs - too many documents in shards ")
        );
    }

    public void testCloneInputs() {
        IndexMetadata indexMetadata = IndexMetadata.builder("index")
            .settings(indexSettings(IndexVersion.current(), randomIntBetween(1, 5), randomIntBetween(0, 5)))
            .build();

        ResizeNumberOfShardsCalculator.CloneShardsCalculator cloneShardsCalculator =
            new ResizeNumberOfShardsCalculator.CloneShardsCalculator();
        assertEquals(indexMetadata.getNumberOfShards(), cloneShardsCalculator.calculate(null, null, indexMetadata));
        assertEquals(
            indexMetadata.getNumberOfShards() + 1,
            cloneShardsCalculator.calculate(indexMetadata.getNumberOfShards() + 1, null, indexMetadata)
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> cloneShardsCalculator.validate(indexMetadata.getNumberOfShards() + 1, indexMetadata)
        );
    }

    public void testSplitInputs() {
        IndexMetadata indexMetadata = IndexMetadata.builder("index")
            .settings(indexSettings(IndexVersion.current(), randomIntBetween(2, 5), randomIntBetween(0, 5)))
            .build();

        ResizeNumberOfShardsCalculator.SplitShardsCalculator splitShardsCalculator =
            new ResizeNumberOfShardsCalculator.SplitShardsCalculator();
        assertEquals(10, splitShardsCalculator.calculate(10, null, indexMetadata));
        expectThrows(AssertionError.class, () -> splitShardsCalculator.calculate(null, null, indexMetadata));
        expectThrows(
            IllegalArgumentException.class,
            () -> splitShardsCalculator.validate(indexMetadata.getNumberOfShards() - 1, indexMetadata)
        );
    }

    public void testCalculateTargetShardsNumberInShrink() {
        assertEquals(1, calculateAcceptableNumberOfShards(0, 0));
        assertEquals(3, calculateAcceptableNumberOfShards(9, 2));
        assertEquals(1, calculateAcceptableNumberOfShards(10, 0));
        assertEquals(1, calculateAcceptableNumberOfShards(10, 1));
        assertEquals(2, calculateAcceptableNumberOfShards(10, 2));
        assertEquals(5, calculateAcceptableNumberOfShards(10, 3));
        assertEquals(10, calculateAcceptableNumberOfShards(10, 6));
        assertEquals(10, calculateAcceptableNumberOfShards(10, 11));
        assertEquals(59, calculateAcceptableNumberOfShards(59, 21));
        assertEquals(30, calculateAcceptableNumberOfShards(60, 21));
        assertEquals(60, calculateAcceptableNumberOfShards(60, 31));
    }
}
