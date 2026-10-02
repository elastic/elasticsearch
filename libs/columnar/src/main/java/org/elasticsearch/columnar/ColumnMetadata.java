/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

import org.apache.lucene.store.DataOutput;
import org.elasticsearch.columnar.numeric.NumericColumnMetadata;
import org.elasticsearch.columnar.string.StringColumnMetadata;

import java.io.IOException;

/**
 * What one column records in the meta stream so it can be reopened. Each column type has its own shape;
 * this is the seam the format-level write and read dispatch holds them by.
 *
 * <p>Sealed so that dispatch is a pattern switch the compiler checks for exhaustiveness: a new column type
 * fails to compile until every dispatch handles it, rather than needing a cast or a null-per-type union.
 */
public sealed interface ColumnMetadata permits NumericColumnMetadata, StringColumnMetadata {

    /** Writes this column's metadata; the matching {@code readFrom} on the implementation reads it back. */
    void writeTo(DataOutput out) throws IOException;
}
