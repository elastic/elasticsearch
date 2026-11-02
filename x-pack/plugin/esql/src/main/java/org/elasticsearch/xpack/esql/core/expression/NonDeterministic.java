/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression;

/**
 * Marker interface for expressions that can evaluate to a different value each time they are evaluated, even on the same input.
 * <p>
 * Optimizer rules must not make the plan evaluate such an expression more often than the query does, for example by copying a
 * condition that contains it or by inlining an alias that defines it: the evaluations could disagree.
 * <p>
 * If a function has this property, it <b>must</b> be marked with this interface, so that those optimizer rules leave it alone.
 */
public interface NonDeterministic {}
