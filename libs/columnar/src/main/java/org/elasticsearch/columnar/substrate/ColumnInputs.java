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
import org.elasticsearch.codec.profiler.MetricRecorder;
import org.elasticsearch.core.Nullable;

/**
 * Where a column is read from, and the recorder its readers report to. See {@link ColumnOutputs} for what each file
 * holds.
 *
 * @param metrics the recorder of the readers built from these inputs, or null when they are not profiled
 */
public record ColumnInputs(
    IndexInput data,
    IndexInput addressing,
    IndexInput lengths,
    IndexInput navigation,
    @Nullable MetricRecorder metrics
) {

    /** A copy of these inputs with the recorder {@code metrics}. */
    public ColumnInputs withMetrics(@Nullable MetricRecorder metrics) {
        return new ColumnInputs(data, addressing, lengths, navigation, metrics);
    }
}
