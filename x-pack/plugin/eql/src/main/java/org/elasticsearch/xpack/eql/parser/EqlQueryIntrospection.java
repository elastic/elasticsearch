/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.parser;

import org.elasticsearch.xpack.eql.plan.logical.AbstractJoin;
import org.elasticsearch.xpack.eql.plan.logical.LimitWithOffset;
import org.elasticsearch.xpack.eql.plan.logical.Sample;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.List;

/**
 * Structural facts about an EQL query string, derivable purely by parsing it: its {@link Mode result mode} and whether
 * it carries its own explicit limit.
 *
 * <p>A small, dependency-light facade for callers outside the EQL engine (notably the ES|QL {@code EQL} source command)
 * that need to reason about a query at plan time without pulling in the {@code ql} logical-plan types. All
 * {@code ql}/{@code eql} plan references are confined to this class; callers see only {@link Mode} and primitive
 * results — never the query's internal shape, so consumers cannot come to depend on it.
 */
public final class EqlQueryIntrospection {

    private EqlQueryIntrospection() {}

    /** The result mode of an EQL query — event, sequence, or sample. */
    public enum Mode {
        /** A plain event query — returns a flat list of matching events. */
        EVENT,
        /** A {@code sequence}/{@code join} query — returns ordered groups of events with join keys. */
        SEQUENCE,
        /** A {@code sample} query — returns groups of events sharing join-key values. */
        SAMPLE
    }

    public record Shape(Mode mode, boolean hasExplicitLimit) {}

    public static Shape inspect(String query) {
        LogicalPlan plan = new EqlParser().createStatement(query);
        return new Shape(mode(plan), hasExplicitLimit(plan));
    }

    private static Mode mode(LogicalPlan plan) {
        // The join node (sequence/sample) is wrapped by top-level nodes (e.g. head/tail ordering, projection),
        // so search down the tree rather than inspecting the root. forEachDown visits top-down; the first match
        // is the outermost join (EQL does not nest sequences/samples).
        List<AbstractJoin> joins = new ArrayList<>(1);
        plan.forEachDown(AbstractJoin.class, joins::add);
        if (joins.isEmpty()) {
            return Mode.EVENT;
        }
        // Sample extends AbstractJoin, so it must be distinguished explicitly.
        return joins.get(0) instanceof Sample ? Mode.SAMPLE : Mode.SEQUENCE;
    }

    private static boolean hasExplicitLimit(LogicalPlan plan) {
        // The parser always inserts one implicit head/tail limit; a user-written | head / | tail adds a second.
        return plan.collect(LimitWithOffset.class::isInstance).size() >= 2;
    }
}
