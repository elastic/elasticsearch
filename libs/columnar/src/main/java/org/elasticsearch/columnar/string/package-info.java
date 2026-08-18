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
 * <p>A column records the {@link org.elasticsearch.columnar.string.StringColumnLayout} it was written with.
 * Only {@code PLAIN} exists today — each value's bytes stored directly — and which layout a segment used is
 * codec-internal, so a later ordinal layout arrives as a new id without changing the read surface. See
 * {@code docs/PLAN.md} for how that layout is meant to be decided.
 */
package org.elasticsearch.columnar.string;
