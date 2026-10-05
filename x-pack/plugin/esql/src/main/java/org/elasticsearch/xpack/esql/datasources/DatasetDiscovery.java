/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.datasources.spi.FileList;

/**
 * What one listing established, and which of the two questions each part of it answers.
 * <p>
 * Resolution lists for the schema. How many files define the columns is still the mode's business — none
 * under a declared mapping, one under {@code first_file_wins}, every file under {@code union_by_name} and
 * {@code strict} — but inferred {@code first_file_wins} and a declared mapping now list the files this
 * query's filters keep, then take a prefix of that set. {@link #schemaListing()} can therefore be a
 * filtered subset. Split discovery needs something else entirely: the files this query must
 * read, with its filters applied and no more of them than its limit requires.
 * <p>
 * Those answers came out of one {@link FileList}, so every consumer read whichever one happened to be there, and
 * the cheap listing had to be gated on "no rows are read" — not because a schema needs a whole dataset, but
 * because the scan's file set does. This type names the two sides so each consumer says which it wants:
 * {@link #schemaListing()} for the columns and the partition columns derived from paths, {@link #scanFileSet()}
 * for the files a query reads and for anything counted over them, such as the dataset's file count and its
 * aggregated statistics — a prefix's count is not a dataset's.
 * <p>
 * Both sides are the same listing here, and {@link #shared} is how that is said. Where they differ is past this
 * type: split discovery swaps in the query's own file set through {@code SplitDiscoveryContext#withScanFileSet},
 * and this names the distinction that swap rests on.
 */
public record DatasetDiscovery(FileList schemaListing, FileList scanFileSet) {

    /**
     * One listing standing for both answers, which is every construction site today. Whether it can serve as the
     * scan's file set is a question about it rather than a promise made here - {@link #schemaListingIsComplete}
     * is what asks.
     */
    public static DatasetDiscovery shared(FileList listing) {
        return new DatasetDiscovery(listing, listing);
    }

    /** Whether the schema's listing is the whole of what the pattern matches, and so usable as a file set. */
    public boolean schemaListingIsComplete() {
        return schemaListing.isTruncated() == false;
    }
}
