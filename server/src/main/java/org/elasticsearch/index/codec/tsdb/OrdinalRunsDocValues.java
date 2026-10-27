/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.elasticsearch.core.Nullable;

import java.io.IOException;

/**
 * Sorted doc values of a secondary index sort field, exposing the first run of documents of each ordinal in the segment.
 * <p>
 * A secondary index sort field whose value determines the values of the sort fields ahead of it has one contiguous run of documents
 * per ordinal, with the runs in an order other than that of the ordinals: {@code _tsid} under the grouped time series sort (see
 * {@code IndexSettings#TIME_SERIES_DIMENSION_GROUP_FIELD}), where the metric name hash and the dimension group are both derived
 * from the dimensions that make up the tsid. The runs let a reader locate the documents of an ordinal in constant time even though
 * the ordinals do not ascend with the document ids, which is what the synthetic {@code _id} lookup needs.
 * <p>
 * Documents of an ordinal outside its first run (strays) are not covered. Under the grouped time series sort those are the delete
 * tombstones: they carry the {@code _tsid} of the deleted document but none of the sort keys ahead of it, so they sort to the end of
 * the segment. They are soft-deleted and are never resolved by an {@code _id} lookup, which only needs the live documents, so not
 * enumerating their synthetic ids is acceptable (see {@code ParsedDocument#deleteTombstone}).
 * <p>
 * The runs are recorded from the ordinals alone, so when the tombstones of the last live {@code _tsid} directly follow its documents
 * the run of that ordinal spans both. Readers that need the live documents only clip such a run at the first document without a value
 * for the primary sort field, see {@code TSDBSyntheticIdDocValuesHolder#liveRunsEnd}.
 */
public interface OrdinalRunsDocValues {

    /**
     * The runs of this field in this segment, or {@code null} if the segment holds none for it: the field is not a secondary index
     * sort field, the segment was written before the runs were introduced, or it has no values for the field.
     */
    @Nullable
    OrdinalRuns ordinalRuns() throws IOException;

    /** The first run of documents of each ordinal. */
    interface OrdinalRuns {

        /** The first document of the run of {@code ord}. */
        int firstDoc(int ord) throws IOException;

        /** The document after the last one of the run of {@code ord}. */
        int endDoc(int ord) throws IOException;
    }
}
