/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.fetch;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.ArrayList;
import java.util.List;

/**
 * Splits the attributes that cross the exchange of a distributed row cut into the ones the coordinator needs as
 * values to perform the cut and the ones it only needs for the rows that survive it.
 * <p>
 * Take {@code FROM idx | WHERE x > 1 | SORT ts DESC | LIMIT 10 | KEEP a, b, ts}. Today every data node sends
 * {@code a}, {@code b} and {@code ts} for its candidate rows. The coordinator needs {@code ts} to pick the global
 * top 10, but it only needs {@code a} and {@code b} for those 10 rows. So {@code ts} is <em>eager</em>, {@code a} and
 * {@code b} are <em>deferred</em>, and {@code x} is <em>local</em>: it is read inside the fragment and never crosses.
 * <p>
 * An attribute of the exchange output is deferred when all of these hold:
 * <ul>
 *     <li>the relation produces it, so a document reference is enough to load it,</li>
 *     <li>{@link Fetchability#isFetchable} accepts it,</li>
 *     <li>the coordinator cut does not read it. A sort key is eager even when the query does not return it.</li>
 * </ul>
 * Everything else that crosses the exchange stays eager, which keeps the fetch partial rather than all or nothing:
 * {@code _score}, values computed inside the fragment, and fields that may be unmapped all keep crossing as values.
 * <p>
 * This is the single-cut form of the analysis. Renames are not followed: a rename after the cut reads its source
 * attribute after the cut, so the source attribute is deferred and the rename runs on the coordinator.
 */
public final class AttributeDemand {

    /**
     * The split of one cut.
     *
     * @param eager    attributes that keep crossing the exchange as values, in exchange order
     * @param deferred attributes loaded after the cut, in exchange order
     * @param local    relation attributes the fragment reads that never cross the exchange, for example the fields of a
     *                 filter. They are reported for diagnostics only, the rewrite does not need them.
     */
    public record Demand(List<Attribute> eager, List<Attribute> deferred, List<Attribute> local) {}

    private AttributeDemand() {}

    /**
     * @param fragmentRoot  the {@link Project} at the root of the fragment. Its output is what crosses the exchange.
     * @param relation      the relation the rows come from
     * @param coordinatorCut the coordinator half of the cut, the parent of the exchange
     */
    public static Demand analyze(Project fragmentRoot, EsRelation relation, PhysicalPlan coordinatorCut) {
        AttributeSet relationOutput = relation.outputSet();
        AttributeSet readByCut = coordinatorCut.references();
        List<Attribute> eager = new ArrayList<>();
        List<Attribute> deferred = new ArrayList<>();
        for (Attribute attribute : fragmentRoot.output()) {
            boolean defer = relationOutput.contains(attribute)
                && Fetchability.isFetchable(attribute)
                && readByCut.contains(attribute) == false;
            (defer ? deferred : eager).add(attribute);
        }

        AttributeSet crossing = fragmentRoot.outputSet();
        AttributeSet.Builder readInFragment = AttributeSet.builder();
        fragmentRoot.child().forEachDown(node -> readInFragment.addAll(node.references()));
        List<Attribute> local = new ArrayList<>();
        for (Attribute attribute : relation.output()) {
            if (readInFragment.contains(attribute) && crossing.contains(attribute) == false) {
                local.add(attribute);
            }
        }
        return new Demand(List.copyOf(eager), List.copyOf(deferred), List.copyOf(local));
    }
}
