/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.xpack.esql.core.expression.Attribute;

import java.util.List;

/**
 * Coordinator-only recipe for placing the {@code _source} fields discovered by {@code unmapped_fields="LOAD_ALL"} into the
 * output schema. Their names are unknown while the plan is built, so the ordering cannot be decided during analysis.
 *
 * <p>Rather than replaying the analyzer's projection logic by hand, the implementation substitutes the discovered fields for
 * the synthetic column in the relation and asks the analyzed plan for its output again: every {@code KEEP}/{@code DROP}/
 * {@code RENAME} re-resolves itself against the widened relation, so the answer comes from the same code that produced the
 * mapped columns. Never serialized - the discovery and the placement both happen on the coordinator.
 */
@FunctionalInterface
public interface UnmappedFieldsOrdering {

    /**
     * The output the query would have had if {@code discoveredLeaves} had been mapped fields all along, in schema order.
     * Leaves are matched by {@link org.elasticsearch.xpack.esql.core.expression.NameId}, so a leaf that shares a name with a
     * real column stays distinguishable from it.
     */
    List<Attribute> order(List<Attribute> discoveredLeaves);
}
