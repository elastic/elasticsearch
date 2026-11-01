/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.numeric;

/**
 * Receives decoded values in bulk from {@link ColumnarNumericBinaryDocValues#bulkLongs}. Values arrive as
 * runs sliced straight out of a decoded block, so a consumer builds a column with one array copy per run
 * instead of a per-document call.
 */
public interface LongBlockSink {

    /** Appends {@code length} values starting at {@code from} within {@code values}. */
    void appendLongs(long[] values, int from, int length);
}
