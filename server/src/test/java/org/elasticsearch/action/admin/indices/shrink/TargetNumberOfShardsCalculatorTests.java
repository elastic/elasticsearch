/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.shrink;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.action.admin.indices.shrink.TargetNumberOfShardsCalculator.Shrink.calculateAcceptableNumberOfShards;

public class TargetNumberOfShardsCalculatorTests extends ESTestCase {

    public void testShrink() {
        Settings settings = Settings.builder()
            .put("index.number_of_shards", randomIntBetween(1, 5))
            .put("index.number_of_replicas", randomIntBetween(0, 5))
            .put("index.version.created", Version.CURRENT)
            .build();
        IndexMetadata indexMetadata = IndexMetadata.builder("index").settings(settings).build();

        TargetNumberOfShardsCalculator.Shrink shrink = new TargetNumberOfShardsCalculator.Shrink(
            new StoreStats(between(1, 100), between(0, 100), between(1, 100)),
            (i) -> new DocsStats(between(1, 1000), between(1, 1000), between(0, 10000))
        );
        assertEquals(4, shrink.calculate(4, null, indexMetadata));
        assertEquals(1, shrink.calculate(null, null, indexMetadata));
        assertEquals(1, shrink.calculate(null, ByteSizeValue.ofGb(50), indexMetadata));
        shrink.verify(1, indexMetadata);
        expectThrows(IllegalArgumentException.class, () -> shrink.calculate(4, ByteSizeValue.ofGb(50), indexMetadata));
        expectThrows(IllegalArgumentException.class, () -> shrink.verify(10, indexMetadata));

        assertTrue(
            expectThrows(
                IllegalStateException.class,
                () -> new TargetNumberOfShardsCalculator.Shrink(
                    new StoreStats(between(1, 100), between(0, 100), between(1, 100)),
                    (i) -> new DocsStats(Integer.MAX_VALUE, between(1, 1000), between(1, 100))
                ).verify(1, indexMetadata)
            ).getMessage().startsWith("Can't merge index with more than [2147483519] docs - too many documents in shards ")
        );
    }

    public void testCloneInputs() {
        Settings settings = Settings.builder()
            .put("index.number_of_shards", randomIntBetween(1, 5))
            .put("index.number_of_replicas", randomIntBetween(0, 5))
            .put("index.version.created", Version.CURRENT)
            .build();
        IndexMetadata indexMetadata = IndexMetadata.builder("index").settings(settings).build();

        TargetNumberOfShardsCalculator.Clone clone = new TargetNumberOfShardsCalculator.Clone();
        assertEquals(indexMetadata.getNumberOfShards(), clone.calculate(null, null, indexMetadata));
        assertEquals(indexMetadata.getNumberOfShards() + 1, clone.calculate(indexMetadata.getNumberOfShards() + 1, null, indexMetadata));
        expectThrows(IllegalArgumentException.class, () -> clone.verify(indexMetadata.getNumberOfShards() + 1, indexMetadata));
    }

    public void testSplitInputs() {
        Settings settings = Settings.builder()
            .put("index.number_of_shards", randomIntBetween(2, 5))
            .put("index.number_of_replicas", randomIntBetween(0, 5))
            .put("index.version.created", Version.CURRENT)
            .build();
        IndexMetadata indexMetadata = IndexMetadata.builder("index").settings(settings).build();

        TargetNumberOfShardsCalculator.Split split = new TargetNumberOfShardsCalculator.Split();
        assertEquals(10, split.calculate(10, null, indexMetadata));
        expectThrows(AssertionError.class, () -> split.calculate(null, null, indexMetadata));
        expectThrows(IllegalArgumentException.class, () -> split.verify(indexMetadata.getNumberOfShards() - 1, indexMetadata));
    }

    public void testCalculateTargetShardsNumberInShrink() {
        assertEquals(calculateAcceptableNumberOfShards(0, 0), 1);
        assertEquals(calculateAcceptableNumberOfShards(10, 0), 1);
        assertEquals(calculateAcceptableNumberOfShards(10, 1), 1);
        assertEquals(calculateAcceptableNumberOfShards(10, 2), 2);
        assertEquals(calculateAcceptableNumberOfShards(10, 3), 5);
        assertEquals(calculateAcceptableNumberOfShards(10, 6), 10);
        assertEquals(calculateAcceptableNumberOfShards(10, 11), 10);
        assertEquals(calculateAcceptableNumberOfShards(59, 21), 59);
        assertEquals(calculateAcceptableNumberOfShards(60, 21), 30);
        assertEquals(calculateAcceptableNumberOfShards(60, 31), 60);
    }
}
