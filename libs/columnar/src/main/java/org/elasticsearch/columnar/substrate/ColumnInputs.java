/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.substrate;

import org.apache.lucene.store.IndexInput;
import org.elasticsearch.lucene.store.PrefetchRanges;

/** Where a column is read from; see {@link ColumnOutputs} for what each file holds. */
public record ColumnInputs(IndexInput data, IndexInput addressing, IndexInput lengths, IndexInput navigation) {

    /** The same inputs, wrapped by {@code prefetches}. */
    public ColumnInputs wrap(PrefetchRanges prefetches) {
        return new ColumnInputs(
            prefetches.wrap(data.clone()),
            prefetches.wrap(addressing.clone()),
            prefetches.wrap(lengths.clone()),
            prefetches.wrap(navigation.clone())
        );
    }
}
