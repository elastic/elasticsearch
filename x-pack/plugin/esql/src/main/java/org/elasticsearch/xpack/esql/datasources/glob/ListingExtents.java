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
 * partition columns and their types. They are separate questions, and both are the dataset's: a mode that
 * answers its schema from one file promises to look at less, and path-derived columns come under the same
 * promise. Neither is the query's — the files a query reads are discovered separately, by split discovery.
 * <p>
 * They were one number, which is why a dataset setting named for partition sampling became the listing bound,
 * and why bounding a listing for one reason silently bounded the other. Both are {@link Integer#MAX_VALUE}
 * when nothing bounds them.
 * <p>
 * Only {@code maxFiles} sets {@link FileList#isTruncated()}: a listing whose partitions were sampled still
 * returns every file the pattern matches, so it is not a prefix of the dataset. A truncated one is, and a
 * query that reads rows may be handed one — {@link FileList#isTruncated()} states what that obliges its
 * readers to do.
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
        // The invariant the sampling rests on, enforced rather than described: a listing must type its partition
        // columns from every file it returns. Sampling fewer ships partition metadata covering a prefix of the
        // listing's own files - types from part of it, and no values at all past the sample. Since partition
        // metadata became ordinal-aligned, values are looked up by listing position, so such a listing asserts on
        // construction (FileList's coversFileCount) or, with assertions off, reads null for the files past the
        // sample and throws for an index into them. Nothing downstream can tell either way.
        //
        // Nothing constructs one: both production sites pass the two equal or both unbounded. The type allowed it,
        // which is the only reason this has to say so.
        if (maxPartitionPaths < maxFiles) {
            throw new IllegalArgumentException(
                "a partition sample [" + maxPartitionPaths + "] cannot be smaller than the file set it types [" + maxFiles + "]"
            );
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
