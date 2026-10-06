/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.fetch;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.expression.function.blockloader.BlockLoaderExpression;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
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
 *     <li>no {@link Eval} of the fragment reads its values. The data node loads those for every row the {@code Eval}
 *     sees, so the fetch would read them a second time. In {@code SORT a + 1 | LIMIT 10 | KEEP a}, the sort key is an
 *     {@code Eval} of {@code a}, so {@code a} is eager.</li>
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
        AttributeSet loadedBeforeTheCut = loadedByEvals(fragmentRoot.child());
        List<Attribute> eager = new ArrayList<>();
        List<Attribute> deferred = new ArrayList<>();
        for (Attribute attribute : fragmentRoot.output()) {
            boolean defer = relationOutput.contains(attribute)
                && Fetchability.isFetchable(attribute)
                && readByCut.contains(attribute) == false
                && loadedBeforeTheCut.contains(attribute) == false;
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

    /**
     * The attributes whose values an {@link Eval} of the fragment reads, so the data node loads them before the cut.
     * <p>
     * A field passed straight to a function the loader may compute itself, like {@code LENGTH(message)}, doesn't count.
     * The data node may load the length and never the message, and only the data node can tell. Filters don't count
     * either: Lucene usually answers them without loading values. Where these guesses are wrong, the column is read
     * twice, once before the cut and once by the fetch.
     */
    private static AttributeSet loadedByEvals(LogicalPlan fragmentBody) {
        AttributeSet.Builder loaded = AttributeSet.builder();
        fragmentBody.forEachDown(Eval.class, eval -> {
            for (Alias field : eval.fields()) {
                collectValueReads(field.child(), loaded);
            }
        });
        return loaded.build();
    }

    private static void collectValueReads(Expression expression, AttributeSet.Builder reads) {
        if (expression instanceof Attribute attribute) {
            reads.add(attribute);
            return;
        }
        boolean mayFuseIntoTheLoad = expression instanceof BlockLoaderExpression;
        for (Expression child : expression.children()) {
            if (mayFuseIntoTheLoad && child instanceof FieldAttribute) {
                continue;
            }
            collectValueReads(child, reads);
        }
    }
}
