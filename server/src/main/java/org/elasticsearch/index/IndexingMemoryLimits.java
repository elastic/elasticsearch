/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.indices.IndexingMemoryController;

/**
 * The heap limits enforced by {@link IndexingPressure} and {@link IndexingMemoryController}.
 * <p>
 * By default these are read from node settings (see {@link SettingsIndexingMemoryLimits}). A distribution can replace
 * that source by providing an {@link IndexingMemoryLimitsFactory}, for example to derive all of the limits from a
 * single memory budget. Limits are read once at node start.
 */
public interface IndexingMemoryLimits {

    /** The limit on the bytes held by in-flight coordinating operations. */
    ByteSizeValue coordinatingLimit();

    /** The limit on the bytes held by in-flight coordinating and primary operations combined. */
    ByteSizeValue primaryLimit();

    /** The limit on the bytes held by in-flight replica operations. */
    ByteSizeValue replicaLimit();

    /** The largest single operation that will be accepted. */
    ByteSizeValue maxOperationSize();

    /** Bulks are split into {@link IndexingPressure#SPLIT_BULK_HIGH_WATERMARK_SIZE} chunks once indexing memory use reaches this. */
    ByteSizeValue splitBulkHighWatermark();

    /** Bulks are split into {@link IndexingPressure#SPLIT_BULK_LOW_WATERMARK_SIZE} chunks once indexing memory use reaches this. */
    ByteSizeValue splitBulkLowWatermark();

    /** The budget for the indexing buffers of all active shards on the node. */
    ByteSizeValue indexBufferSize();
}
