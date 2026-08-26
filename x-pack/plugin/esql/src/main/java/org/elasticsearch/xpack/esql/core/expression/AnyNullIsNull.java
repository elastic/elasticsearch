/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression;

/**
 * Marker interface for expressions that propagate {@code null} through <b>all</b>
 * of their arguments: if any argument evaluates to {@code null} at a given position,
 * the expression itself evaluates to {@code null} at that position.
 * <p>
 * If a function has this property, it <b>must</b> be marked with this interface,
 * so that optimizer rules can apply.
 */
public interface AnyNullIsNull {}
