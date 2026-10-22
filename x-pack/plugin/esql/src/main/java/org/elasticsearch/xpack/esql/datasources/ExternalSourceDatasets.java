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
 * Both node kinds name the dataset they were planned from. The names are what a receiving node authorizes before it
 * rebinds the source to its own registry ({@link DatasetConfigResolver}): a plan arriving at a node is not trusted to
 * have been built by a coordinator the caller was allowed to use, so the caller's credential must cover every dataset
 * the plan names. A node with no name at all cannot be authorized by name; only the snapshot-only inline
 * {@code EXTERNAL} command plans such a node, and its presence is reported separately so the caller can apply the
 * release/snapshot rule {@link DatasetConfigResolver#rebind} applies.
 */
public final class ExternalSourceDatasets {

    private static final ExternalSourceDatasets NONE = new ExternalSourceDatasets(Set.of(), false);

    private final Set<String> names;
    private final boolean unnamed;

    private ExternalSourceDatasets(Set<String> names, boolean unnamed) {
        this.names = names;
        this.unnamed = unnamed;
    }

    /** Walks the whole tree; {@code null} stands for a plan that was not sent (an absent lookup pre-join plan). */
    public static ExternalSourceDatasets of(@Nullable PhysicalPlan plan) {
        if (plan == null) {
            return NONE;
        }
        Set<String> names = new LinkedHashSet<>();
        boolean[] unnamed = new boolean[1];
        plan.forEachDown(node -> {
            if (node instanceof ExternalSourceExec exec) {
                collect(exec.datasetName(), names, unnamed);
            } else if (node instanceof FragmentExec fragment) {
                fragment.fragment().forEachDown(ExternalRelation.class, relation -> collect(relation.datasetName(), names, unnamed));
            }
        });
        if (names.isEmpty() && unnamed[0] == false) {
            return NONE;
        }
        return new ExternalSourceDatasets(Collections.unmodifiableSet(names), unnamed[0]);
    }

    /** Whether the plan reads any external source at all, named or not. */
    public static boolean hasExternalNode(@Nullable PhysicalPlan plan) {
        return of(plan).isEmpty() == false;
    }

    private static void collect(@Nullable String datasetName, Set<String> names, boolean[] unnamed) {
        if (datasetName == null) {
            unnamed[0] = true;
        } else {
            names.add(datasetName);
        }
    }

    /** {@code true} when the plan has no external node. */
    public boolean isEmpty() {
        return names.isEmpty() && unnamed == false;
    }

    /** The dataset names the plan's external nodes carry, in plan order, without duplicates. */
    public Set<String> names() {
        return names;
    }

    /** Whether some external node carries no dataset name. */
    public boolean hasUnnamed() {
        return unnamed;
    }
}
