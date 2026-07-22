/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.parser;

import org.elasticsearch.xpack.eql.plan.logical.AbstractJoin;
import org.elasticsearch.xpack.eql.plan.logical.Sample;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.List;

/**
 * The result mode of an EQL query — event, sequence, or sample — derivable purely by parsing the
 * query string.
 *
 * <p>This is a small, dependency-light API intended for callers outside the EQL engine (notably the
 * ES|QL {@code EQL} source command) that need to fix an output schema at plan time without pulling in
 * the {@code ql} logical-plan types. All {@code ql}/{@code eql} plan references are confined to this
 * class; callers see only this enum. Only the mode is exposed — no per-stage structural detail — so
 * consumers cannot come to depend on the query's internal shape.
 */
public enum EqlQueryMode {
    /** A plain event query — returns a flat list of matching events. */
    EVENT,
    /** A {@code sequence}/{@code join} query — returns ordered groups of events with join keys. */
    SEQUENCE,
    /** A {@code sample} query — returns groups of events sharing join-key values. */
    SAMPLE;

    /**
     * Parses {@code query} and returns its result mode. Throws the same exceptions as
     * {@link EqlParser#createStatement(String)} for malformed queries.
     *
     * @param query the raw EQL query string
     * @return the result mode
     */
    public static EqlQueryMode of(String query) {
        LogicalPlan plan = new EqlParser().createStatement(query);
        // The join node (sequence/sample) is wrapped by top-level nodes (e.g. head/tail ordering, projection),
        // so search down the tree rather than inspecting the root. forEachDown visits top-down; the first match
        // is the outermost join (EQL does not nest sequences/samples).
        List<AbstractJoin> joins = new ArrayList<>(1);
        plan.forEachDown(AbstractJoin.class, joins::add);
        if (joins.isEmpty()) {
            return EVENT;
        }
        // Sample extends AbstractJoin, so it must be distinguished explicitly.
        return joins.get(0) instanceof Sample ? SAMPLE : SEQUENCE;
    }
}
