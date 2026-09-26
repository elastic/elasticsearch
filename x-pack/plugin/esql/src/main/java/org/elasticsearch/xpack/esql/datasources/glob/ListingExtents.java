/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.glob;

import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;

import java.util.List;

/**
 * How far a listing runs, for each of the two things that read it.
 * <p>
 * {@code maxFiles} bounds the files the expansion returns — what split discovery reads and what the listing
 * cache may hold. {@code maxPartitionPaths} bounds the paths partition detection folds over to decide the
 * partition columns and their types. They are separate questions with separate owners: the first is the
 * query's ({@code LIMIT 0} needs no files at all), the second the dataset's (a mode that answers its schema
 * from one file promises to look at less, and path-derived columns come under the same promise).
 * <p>
 * They were one number, which is why a dataset setting named for partition sampling became the listing bound,
 * and why bounding a listing for one reason silently bounded the other. Both are {@link Integer#MAX_VALUE}
 * when nothing bounds them.
 * <p>
 * Only {@code fileSet} sets {@link FileList#isTruncated()}: a listing whose partitions were sampled still
 * returns every file the pattern matches, so it is not a prefix of the dataset and the invariants that keep a
 * prefix away from a reading query do not apply to it.
 */
public record ListingExtents(int maxFiles, int maxPartitionPaths) {

    /** Neither bounded: the whole glob, and every path folded for partitions. */
    public static final ListingExtents UNBOUNDED = new ListingExtents(Integer.MAX_VALUE, Integer.MAX_VALUE);

    public ListingExtents {
        if (maxFiles < 1) {
            throw new IllegalArgumentException("maxFiles must be positive, got [" + maxFiles + "]");
        }
        if (maxPartitionPaths < 1) {
            throw new IllegalArgumentException("maxPartitionPaths must be positive, got [" + maxPartitionPaths + "]");
        }
        // The invariant the sampling rests on, enforced rather than described: a listing that returns every file
        // the pattern matches must type its partition columns from every one of them. Sampling such a listing
        // ships partition metadata covering a prefix of its own files - types from part of the dataset, and no
        // partition values at all past the sample - and nothing downstream can tell.
        if (maxFiles == Integer.MAX_VALUE && maxPartitionPaths != Integer.MAX_VALUE) {
            throw new IllegalArgumentException("an unbounded file set cannot carry a bounded partition sample");
        }
    }

    public boolean boundsFileSet() {
        return maxFiles != Integer.MAX_VALUE;
    }

    /**
     * The prefix of {@code files} partition detection may fold over. Listing order, because that is the order the
     * dataset's own settings put the files in, so the sample is the front of the dataset rather than an arbitrary
     * subset of it. A view of {@code files}, not a copy: it must not be mutated, and it must not outlive them.
     * <p>
     * Only ever a prefix where the file set was itself bounded — the constructor refuses the combination that
     * would make this sample a full listing.
     */
    public List<StorageEntry> partitionSampleOf(List<StorageEntry> files) {
        return files.size() <= maxPartitionPaths ? files : files.subList(0, maxPartitionPaths);
    }
}
