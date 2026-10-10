/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan;

import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

/**
 * A single {@code LET name = (subquery)} binding produced by parsing one {@code LET} prefix statement. A query may have several
 * {@code LET} statements, each declaring exactly one binding.
 * <p>
 * Bindings are evaluated in declaration order: binding <em>N</em> may reference bindings
 * <em>1..N-1</em> by name. The plans held here are unresolved — they contain
 * {@link org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation} nodes for any
 * referenced names, which {@link org.elasticsearch.xpack.esql.analysis.LetResolver}
 * replaces before view resolution begins.
 * </p>
 *
 * @param source the source location of the binding in the query text
 * @param name   the identifier used to reference this binding in the main query
 * @param plan   the unresolved logical plan of the binding's subquery
 */
public record LetBinding(Source source, String name, LogicalPlan plan) {}
