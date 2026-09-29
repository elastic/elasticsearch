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

    /**
     * A sample smaller than the file set it types is refused. It would ship partition metadata covering a prefix
     * of the listing's own files, and since that metadata became ordinal-aligned, the files past the sample read
     * null or throw on an index into them depending only on whether assertions are on.
     * <p>
     * The unbounded case is the same rule: every file the pattern matches must be typed from every one of them.
     */
    public void testASampleSmallerThanTheFileSetItTypesIsRefused() {
        IllegalArgumentException narrower = expectThrows(IllegalArgumentException.class, () -> new ListingExtents(3, 2));
        assertThat(narrower.getMessage(), containsString("cannot be smaller than the file set it types"));

        IllegalArgumentException unbounded = expectThrows(
            IllegalArgumentException.class,
            () -> new ListingExtents(Integer.MAX_VALUE, 1000)
        );
        assertThat(unbounded.getMessage(), containsString("cannot be smaller than the file set it types"));

        // Equal is what both production sites pass, and a sample wider than the file set is harmless.
        new ListingExtents(3, 3);
        new ListingExtents(3, 4);
    }

}
