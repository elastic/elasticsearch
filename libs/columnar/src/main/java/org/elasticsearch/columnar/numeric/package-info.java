/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

/**
 * Numeric (long-valued) columns on the ColumNAR substrate. Values are grouped into fixed-size,
 * doc-id-aligned blocks; each block is encoded by {@link NumericBlockEncoder} (delta/offset/GCD then
 * a {@link ForUtil} bit-packed payload).
 *
 * <p>Single- and multi-valued numerics share one format ({@link NumericColumnWriter}): values are
 * stored in written order, and a per-document value-address table is added only when a column is
 * multi-valued.
 */
package org.elasticsearch.columnar.numeric;
