/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression;

/**
 * Marker interface for functions that return exactly the same values on every node, JVM and version, such as integer
 * arithmetic or {@code DATE_TRUNC} in UTC. Only these functions can compute index sort keys, which one node computes when it
 * writes a segment and another when it merges it.
 * <p>
 * Functions that use {@link Math} for floating point results, or the JVM's Unicode, locale or time zone data, aren't
 * reproducible.
 * <p>
 * <b>Indices are sorted on marked functions: their results can't change, even to fix a bug, they can't be unmarked, and rules
 * that rewrite them must compute the same values.</b>
 */
public interface Reproducible {}
