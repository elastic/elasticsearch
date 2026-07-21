/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

/**
 * A type-safe, JSON-like data model usable as an intermediate representation for structured records such as audit or
 * query logs.
 *
 * <p>The model is a sealed {@link org.elasticsearch.datatree.DataValue} hierarchy. Scalars are immutable records
 * ({@link org.elasticsearch.datatree.DataNull}, {@link org.elasticsearch.datatree.DataBoolean},
 * {@link org.elasticsearch.datatree.DataString}, and the numeric types below); containers
 * ({@link org.elasticsearch.datatree.DataObject} and
 * {@link org.elasticsearch.datatree.DataArray}) are mutable so callers can assemble a tree incrementally.
 *
 * <h2>Numbers</h2>
 * A value narrows to {@link org.elasticsearch.datatree.DataLong} or
 * {@link org.elasticsearch.datatree.DataDouble} when it fits, and otherwise retains full precision as
 * {@link org.elasticsearch.datatree.DataInteger} (backed by {@link java.math.BigInteger}) or
 * {@link org.elasticsearch.datatree.DataDecimal} (backed by {@link java.math.BigDecimal}). This lets large
 * integers that exceed {@code long} range continue to render as bare JSON numbers. Non-finite doubles are not valid JSON and are rejected.
 *
 * <h2>Rendering is a terminal concern</h2>
 * The model never flattens or serializes itself. Producers build a tree that preserves structure; a converter decides how to render it
 * for a given sink. In particular, collapsing nested objects and arrays to compact JSON happens only at that terminal stage, not while
 * the tree is being assembled.
 *
 * <h2>Helpers</h2>
 * {@link org.elasticsearch.datatree.DataValues} converts to and from plain Java objects and maps, and
 * {@link org.elasticsearch.datatree.DataPath} provides read-only navigation.
 */
package org.elasticsearch.datatree;
