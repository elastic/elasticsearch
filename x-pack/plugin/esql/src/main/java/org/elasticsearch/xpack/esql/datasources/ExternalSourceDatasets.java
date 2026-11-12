/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * The external sources a physical plan reads, as seen by a node that received the plan over the wire: every
 * {@link ExternalSourceExec} in the physical tree and every {@link ExternalRelation} inside a {@link FragmentExec}'s
 * logical fragment, wherever they sit (under an exchange, in a {@code UnionAll} branch, on the right side of a join).
 * <p>
 * A node naming a <b>registered</b> dataset names what the receiving node authorizes before rebinding the source to
 * its own registry ({@link DatasetConfigResolver}): a plan arriving at a node is not trusted to have been built by a
 * coordinator the caller was allowed to use, so the caller's credential must cover every dataset the plan names.
 * <p>
 * The other two kinds name no dataset and so are authorized by nothing here, and each is reported separately because
 * each decides something on its own. A node carrying the <b>definition itself</b> is still counted, so that a plan
 * holding only such nodes is not {@link #isEmpty()} — the refusals keyed on that, a cross-cluster request and a
 * request with neither shards nor external splits, would otherwise let one through. A node with neither a name nor a
 * definition is planned on snapshot builds alone, and its presence is reported so the caller can apply the
 * release/snapshot rule {@link DatasetConfigResolver#rebind} applies.
 */
public final class ExternalSourceDatasets {

    private static final ExternalSourceDatasets NONE = new ExternalSourceDatasets(Set.of(), Set.of(), false);

    private final Set<String> names;
    private final Set<String> dataSources;
    private final boolean unnamed;

    private ExternalSourceDatasets(Set<String> names, Set<String> dataSources, boolean unnamed) {
        this.names = names;
        this.dataSources = dataSources;
        this.unnamed = unnamed;
    }

    /** Walks the whole tree; {@code null} stands for a plan that was not sent (an absent lookup pre-join plan). */
    public static ExternalSourceDatasets of(@Nullable PhysicalPlan plan) {
        if (plan == null) {
            return NONE;
        }
        Set<String> names = new LinkedHashSet<>();
        Set<String> dataSources = new LinkedHashSet<>();
        boolean[] unnamed = new boolean[1];
        plan.forEachDown(node -> {
            if (node instanceof ExternalSourceExec exec) {
                collect(exec.datasetReference(), names, dataSources, unnamed);
            } else if (node instanceof FragmentExec fragment) {
                fragment.fragment()
                    .forEachDown(ExternalRelation.class, relation -> collect(relation.datasetReference(), names, dataSources, unnamed));
            }
        });
        if (names.isEmpty() && dataSources.isEmpty() && unnamed[0] == false) {
            return NONE;
        }
        return new ExternalSourceDatasets(Collections.unmodifiableSet(names), Collections.unmodifiableSet(dataSources), unnamed[0]);
    }

    /** Whether the plan reads any external source at all, named or not. */
    public static boolean hasExternalNode(@Nullable PhysicalPlan plan) {
        return of(plan).isEmpty() == false;
    }

    private static void collect(@Nullable DatasetReference reference, Set<String> names, Set<String> dataSources, boolean[] unnamed) {
        if (reference == null) {
            unnamed[0] = true;
        } else if (reference instanceof DatasetReference.Named named) {
            names.add(named.name());
        } else {
            dataSources.add(((DatasetReference.Inline) reference).dataSource());
        }
    }

    /** {@code true} when the plan has no external node. */
    public boolean isEmpty() {
        return names.isEmpty() && dataSources.isEmpty() && unnamed == false;
    }

    /** The dataset names the plan's external nodes carry, in plan order, without duplicates. */
    public Set<String> names() {
        return names;
    }

    /**
     * The data sources the plan reads a carried definition through, in plan order, without duplicates. Nothing
     * authorizes them: the set exists so a plan holding only carried definitions is not {@link #isEmpty()}.
     */
    public Set<String> dataSources() {
        return dataSources;
    }

    /** Whether some external node has neither a dataset name nor a definition of its own. */
    public boolean hasUnnamed() {
        return unnamed;
    }
}
