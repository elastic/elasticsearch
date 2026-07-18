/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/**
 * A type-safe, JSON-like data model used as the intermediate representation for audit log entries.
 *
 * <p>The model is a sealed {@link org.elasticsearch.xpack.core.security.audit.data.DataValue} hierarchy. Scalars are immutable records
 * ({@link org.elasticsearch.xpack.core.security.audit.data.DataNull}, {@link org.elasticsearch.xpack.core.security.audit.data.DataBoolean},
 * {@link org.elasticsearch.xpack.core.security.audit.data.DataString}, and the numeric types below); containers
 * ({@link org.elasticsearch.xpack.core.security.audit.data.DataObject} and
 * {@link org.elasticsearch.xpack.core.security.audit.data.DataArray}) are mutable so callers can assemble a tree incrementally.
 *
 * <h2>Numbers</h2>
 * A value narrows to {@link org.elasticsearch.xpack.core.security.audit.data.DataLong} or
 * {@link org.elasticsearch.xpack.core.security.audit.data.DataDouble} when it fits, and otherwise retains full precision as
 * {@link org.elasticsearch.xpack.core.security.audit.data.DataInteger} (backed by {@link java.math.BigInteger}) or
 * {@link org.elasticsearch.xpack.core.security.audit.data.DataDecimal} (backed by {@link java.math.BigDecimal}). This lets large
 * integers that exceed {@code long} range continue to render as bare JSON numbers. Non-finite doubles are not valid JSON and are rejected.
 *
 * <h2>Rendering is a terminal concern</h2>
 * The model never flattens or serializes itself. Producers build a tree that preserves structure; a converter decides how to render it
 * for a given sink. In particular, collapsing nested objects and arrays to compact JSON happens only at that terminal stage, not while
 * the tree is being assembled.
 *
 * <h2>Helpers</h2>
 * {@link org.elasticsearch.xpack.core.security.audit.data.DataValues} converts to and from plain Java objects and maps,
 * {@link org.elasticsearch.xpack.core.security.audit.data.XContentData} builds a tree from XContent, and
 * {@link org.elasticsearch.xpack.core.security.audit.data.DataPath} provides read-only navigation.
 */
package org.elasticsearch.xpack.core.security.audit.data;
