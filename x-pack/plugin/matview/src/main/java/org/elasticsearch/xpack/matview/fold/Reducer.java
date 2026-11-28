/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.matview.fold;

import org.apache.lucene.index.CodecReader;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Releasable;

import java.io.IOException;
import java.util.List;

/**
 * Computes the reduced values of every group found in the live documents of a set of segments. The merge
 * machinery asks the {@link Result} for the value a representative document must carry in the merged segment.
 * Keeping this behind an interface keeps the Lucene-side folding independent of the engine that does the math.
 */
public interface Reducer {

    /** The group keys the fold scan registered, {@link KeyEncoder} encoded, each with a dense id. */
    interface KeyIndex {
        /** Id of the key, or a negative number when it was never registered. */
        long find(BytesRef key);

        long size();
    }

    /**
     * Reduce the live documents of the given readers. The readers stay open and owned by the caller.
     */
    Result reduce(List<? extends CodecReader> readers, KeyIndex keys) throws IOException;

    interface Result extends Releasable {
        /** Row holding the reduced values of the group with the given key id, or -1 when the group was not seen. */
        int rowOf(int keyId);

        /** Whether the reduced field has a value in the given row; a group may have no value for a field. */
        boolean hasValue(int field, int row);

        /** Reduced value of the field in the row, encoded exactly as the field's doc values store it. */
        long value(int field, int row);
    }
}
