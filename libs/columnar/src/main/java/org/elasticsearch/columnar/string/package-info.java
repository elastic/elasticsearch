/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

/**
 * The adaptive string (keyword) column: an ordinal-indexed, block-encoded store of variable-length byte
 * values on the shared binary substrate.
 *
 * <p>Each segment picks its own {@link org.elasticsearch.columnar.string.StringColumnLayout} from that
 * segment's cardinality — values stored directly when cardinality is high, or a per-segment terms dictionary
 * plus one ordinal per value when it is low enough to fit
 * {@link org.elasticsearch.columnar.string.StringDictionary#MAX_SIZE}. The choice is codec-internal: both
 * layouts are served at the same {@code BINARY} surface through
 * {@link org.elasticsearch.columnar.string.ColumnarStringBinaryDocValues}, so ordinals never surface and only
 * the segments that chose a dictionary carry one.
 *
 * <p>A dictionary column's ordinal stream is encoded with the numeric column's own
 * {@link org.elasticsearch.columnar.numeric.NumericPipeline}, so repeated and sequential ordinal runs collapse
 * through the existing delta / offset / GCD detection and FOR bit-packing rather than a string-specific
 * encoder.
 */
package org.elasticsearch.columnar.string;
