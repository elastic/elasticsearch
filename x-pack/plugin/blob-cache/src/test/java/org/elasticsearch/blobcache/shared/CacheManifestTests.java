/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache.shared;

import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.BACKFILL_IN_PROGRESS_TIMESTAMP;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.UNKNOWN_TIMESTAMP;
import static org.hamcrest.Matchers.equalTo;

public class CacheManifestTests extends ESTestCase {

    private record Key(ShardId shardId, String file) implements SharedBlobCacheService.KeyBase {}

    private static Key randomKey() {
        return new Key(new ShardId(randomAlphaOfLength(8), randomAlphaOfLength(8), randomIntBetween(0, 5)), randomAlphaOfLength(8));
    }

    public void testNormalizeJoinsOverlappingAndAdjacentRanges() {
        assertThat(
            CacheManifest.normalize(List.of(ByteRange.of(40, 50), ByteRange.of(0, 10), ByteRange.of(10, 20), ByteRange.of(15, 25))),
            equalTo(List.of(ByteRange.of(0, 25), ByteRange.of(40, 50)))
        );
        assertThat(CacheManifest.normalize(List.of(ByteRange.of(0, 30), ByteRange.of(5, 10))), equalTo(List.of(ByteRange.of(0, 30))));
        assertThat(CacheManifest.normalize(List.of(ByteRange.EMPTY)), equalTo(List.of()));
    }

    public void testRegionCovers() {
        final var region = new CacheManifest.Region(1L, List.of(ByteRange.of(0, 20), ByteRange.of(40, 60)));

        assertTrue(region.covers(List.of()));
        assertTrue(region.covers(List.of(ByteRange.of(0, 20), ByteRange.of(45, 50))));
        // adjacent pieces of a covered range count as one range
        assertTrue(region.covers(List.of(ByteRange.of(0, 10), ByteRange.of(10, 20))));
        assertFalse(region.covers(List.of(ByteRange.of(0, 25))));
        assertFalse(region.covers(List.of(ByteRange.of(15, 45))));
        assertFalse(region.covers(List.of(ByteRange.of(70, 80))));
    }

    public void testCoversAgreesWithCheckingEveryCandidateAgainstEveryRange() {
        for (int iteration = 0; iteration < 200; iteration++) {
            final var region = new CacheManifest.Region(0L, randomRanges(randomIntBetween(0, 8)));
            final var candidates = randomRanges(randomIntBetween(0, 6));

            final boolean expected = CacheManifest.normalize(candidates)
                .stream()
                .allMatch(candidate -> region.ranges().stream().anyMatch(candidate::isSubRangeOf));

            assertThat(region.ranges() + " covers " + candidates, region.covers(candidates), equalTo(expected));
        }
    }

    private static List<ByteRange> randomRanges(int count) {
        final List<ByteRange> ranges = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            final long start = randomLongBetween(0, 100);
            ranges.add(ByteRange.of(start, start + randomLongBetween(0, 30)));
        }
        return ranges;
    }

    public void testRegionJoinsRangesOnConstruction() {
        final var region = new CacheManifest.Region(1L, List.of(ByteRange.of(10, 20), ByteRange.of(0, 10), ByteRange.of(40, 60)));

        assertThat(region.ranges(), equalTo(List.of(ByteRange.of(0, 20), ByteRange.of(40, 60))));
        assertThat(region.totalBytes(), equalTo(40L));
        // a candidate that spans the join of two touching ranges is covered
        assertTrue(region.covers(List.of(ByteRange.of(5, 15))));
        assertThat(region, equalTo(new CacheManifest.Region(1L, List.of(ByteRange.of(0, 20), ByteRange.of(40, 60)))));
    }

    public void testBuilder() {
        final var key = randomKey();
        final var manifest = CacheManifest.<Key>builder()
            .add(key, 3, 42L, List.of(ByteRange.of(10, 20), ByteRange.of(0, 10)))
            .add(key, 1, BACKFILL_IN_PROGRESS_TIMESTAMP, List.of(ByteRange.of(0, 5)))
            .add(key, 2, 7L, List.of())
            .build();

        assertThat(manifest.get(key, 3), equalTo(new CacheManifest.Region(42L, List.of(ByteRange.of(0, 20)))));
        assertThat(manifest.get(key, 1).timestampMillis(), equalTo(UNKNOWN_TIMESTAMP));
        assertNull("regions without ranges are not recorded", manifest.get(key, 2));
        assertNull(manifest.get(randomKey(), 3));
        assertThat(manifest.regionCount(), equalTo(2));
        assertThat(manifest.totalBytes(), equalTo(25L));
        assertThat(List.copyOf(manifest.regions().get(key).keySet()), equalTo(List.of(1, 3)));
    }

    public void testEquality() {
        final var key = randomKey();
        final var first = CacheManifest.<Key>builder().add(key, 0, 1L, List.of(ByteRange.of(0, 5), ByteRange.of(5, 10))).build();
        final var second = CacheManifest.<Key>builder().add(key, 0, 1L, List.of(ByteRange.of(0, 10))).build();
        final var other = CacheManifest.<Key>builder().add(key, 0, 2L, List.of(ByteRange.of(0, 10))).build();

        assertThat(first, equalTo(second));
        assertThat(first.hashCode(), equalTo(second.hashCode()));
        assertNotEquals(first, other);
    }
}
