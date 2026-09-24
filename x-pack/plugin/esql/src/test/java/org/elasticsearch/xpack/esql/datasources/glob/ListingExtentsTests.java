/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.glob;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.time.Instant;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class ListingExtentsTests extends ESTestCase {

    public void testUnboundedBoundsNeither() {
        assertFalse(ListingExtents.UNBOUNDED.boundsFileSet());
        assertEquals(Integer.MAX_VALUE, ListingExtents.UNBOUNDED.maxPartitionPaths());
    }

    public void testAnExtentMustBeAtLeastOne() {
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> new ListingExtents(0, 1)).getMessage(),
            containsString("maxFiles must be positive")
        );
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> new ListingExtents(1, -3)).getMessage(),
            containsString("maxPartitionPaths must be positive")
        );
    }

    /**
     * The combination that would sample a listing returning every file: partition columns typed from part of a
     * dataset the listing shipped whole, and no partition values at all past the sample. Refused rather than
     * documented, because the two sites that did it were found in review rather than by reading.
     */
    public void testAnUnboundedFileSetCannotCarryABoundedSample() {
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> new ListingExtents(Integer.MAX_VALUE, 10)).getMessage(),
            containsString("an unbounded file set cannot carry a bounded partition sample")
        );
    }

    public void testBoundsFileSetIsTrueOnlyWhenTheFileSetIsCapped() {
        assertTrue(new ListingExtents(10, 10).boundsFileSet());
        assertFalse(ListingExtents.UNBOUNDED.boundsFileSet());
    }

    public void testPartitionSampleOfKeepsTheListWhenItFitsAndTheFrontWhenItDoesNot() {
        List<StorageEntry> files = List.of(entry("a"), entry("b"), entry("c"));

        assertSame("nothing to sample, so nothing is copied", files, ListingExtents.UNBOUNDED.partitionSampleOf(files));
        assertSame(files, new ListingExtents(3, 3).partitionSampleOf(files));
        assertEquals(List.of(entry("a"), entry("b")), new ListingExtents(2, 2).partitionSampleOf(files));
    }

    private static StorageEntry entry(String name) {
        return new StorageEntry(StoragePath.of("s3://bucket/data/" + name + ".parquet"), 100, Instant.EPOCH);
    }
}
