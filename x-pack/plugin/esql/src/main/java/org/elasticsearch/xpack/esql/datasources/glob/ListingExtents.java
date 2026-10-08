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
 * How far a listing runs.
 * <p>
 * One number, because the tree can only express one. It bounds the files the expansion returns - what split
 * discovery reads and what the listing cache may hold - and, being the front of that listing, it is also the set
 * partition detection folds over to decide the partition columns and their types. Both are the dataset's
 * question: a mode that answers its schema from one file promises to look at less, and path-derived columns come
 * under the same promise. Neither is the query's - the files a query reads are discovered separately, by split
 * discovery.
 * <p>
 * It was once two numbers. That separation cannot hold now that partition metadata is ordinal-aligned: values are
 * looked up by listing position, so metadata typed from fewer paths than the listing returns no longer covers it
 * (see {@code PartitionMetadata#coversFileCount}). A second, narrower bound would have described a listing that
 * cannot be built, so it is gone rather than documented.
 * <p>
 * {@link Integer#MAX_VALUE} is unbounded. A bounded listing sets {@link FileList#isTruncated()}, which states what
 * a reader handed a prefix of a dataset owes it.
 */
public record ListingExtents(int maxFiles) {

    /** Neither bounded: the whole glob, and every path folded for partitions. */
    public static final ListingExtents UNBOUNDED = new ListingExtents(Integer.MAX_VALUE);

    public ListingExtents {
        if (maxFiles < 1) {
            throw new IllegalArgumentException("maxFiles must be positive, got [" + maxFiles + "]");
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
        return files.size() <= maxFiles ? files : files.subList(0, maxFiles);
    }
}
