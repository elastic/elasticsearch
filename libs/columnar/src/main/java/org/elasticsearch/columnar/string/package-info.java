/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

/**
 * The string (keyword) column: a value-address-indexed, block-encoded store of variable-length byte values on
 * the shared binary substrate, served at the {@code BINARY} surface through
 * {@link org.elasticsearch.columnar.string.ColumnarStringBinaryDocValues}.
 *
 * <p>A column records the {@link org.elasticsearch.columnar.string.StringColumnLayout} it was written with:
 * {@code PLAIN} stores each value's bytes directly, {@code DICTIONARY} an ordinal per value into the terms
 * the column repeats. Which one a segment used is codec-internal, so it does not change the read surface.
 */
package org.elasticsearch.columnar.string;
