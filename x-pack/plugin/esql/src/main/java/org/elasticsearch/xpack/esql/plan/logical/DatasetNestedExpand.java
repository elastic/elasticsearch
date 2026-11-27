/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NonDeterministic;
import org.elasticsearch.xpack.esql.datasources.DeclaredReadSpec;
import org.elasticsearch.xpack.esql.datasources.FileMetadataColumns;
import org.elasticsearch.xpack.esql.plan.logical.join.LookupJoin;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Which {@code NESTED_EXPAND} over a dataset a query may run, decided on the coordinator over the logical plan, where the
 * {@code Mapper} refuses an index's expansion that cannot run on the data nodes. A dataset has no child documents to
 * read the elements through, so the scan reads the collection at element grain (see
 * {@code PushNestedExpandToExternalSource}), and every node between the expansion and the scan then sees element rows
 * rather than parent rows. Only a node that computes each row from that row alone may sit there; anything else is
 * refused here by name, before the plan reaches a data node, so the refusal is the query's error under every setting,
 * {@code allow_partial_results} included.
 * <p>
 * The checks over the plan's shared parts (expressions, attributes, the declared renames) are the static predicates
 * below, which the physical rule asserts again as invariants over the plan it rewrites.
 */
public final class DatasetNestedExpand {

    private DatasetNestedExpand() {}

    /** Refuses every {@code NESTED_EXPAND} over a dataset in {@code plan} that the scan cannot read. */
    public static void refuseUnsupported(LogicalPlan plan) {
        plan.forEachDown(NestedExpand.class, DatasetNestedExpand::refuse);
    }

    private static void refuse(NestedExpand expand) {
        List<LogicalPlan> chain = new ArrayList<>();
        LogicalPlan node = expand.child();
        while (node instanceof ExternalRelation == false) {
            if (node instanceof UnaryPlan unary) {
                chain.add(node);
                node = unary.child();
            } else if (node instanceof LookupJoin join) {
                chain.add(node);
                node = join.left();
            } else if (node.anyMatch(ExternalRelation.class::isInstance)) {
                // A branch that reads a dataset, beside an index or another dataset: no one scan can read the elements
                throw new VerificationException("NESTED_EXPAND over a dataset cannot follow [{}]", node.sourceText());
            } else {
                // Not over a dataset: an index's expansion is read through its child documents
                return;
            }
        }
        ExternalRelation relation = (ExternalRelation) node;
        for (LogicalPlan between : chain) {
            if (between instanceof NestedExpand lower) {
                throw new VerificationException(
                    "NESTED_EXPAND over a dataset reads one collection per scan, so [{}] cannot also expand [{}]",
                    expand.sourceText(),
                    lower.path()
                );
            }
        }
        if (readsRecordRef(relation.output())) {
            throw new VerificationException(
                "NESTED_EXPAND over a dataset cannot read [{}]: a record reference addresses a file row",
                FileMetadataColumns.RECORD_REF
            );
        }
        Map.Entry<String, String> rename = renameOf(relation.declaredReadSpec(), expand.path());
        if (rename != null) {
            throw new VerificationException(
                "NESTED_EXPAND over a dataset cannot expand [{}]: the dataset declares a rename of [{}] to [{}]",
                expand.path(),
                rename.getKey(),
                rename.getValue()
            );
        }
        Set<String> expandedNames = names(expand.expanded());
        String clash = clashingName(relation.output(), expandedNames);
        if (clash != null) {
            throw new VerificationException(
                "NESTED_EXPAND over a dataset cannot expand [{}]: the dataset has a column named [{}]",
                expand.path(),
                clash
            );
        }
        for (LogicalPlan between : chain) {
            if (pushesThrough(between) == false) {
                throw new VerificationException("NESTED_EXPAND over a dataset cannot follow [{}]", between.sourceText());
            }
            clash = clashingName(between.output(), expandedNames);
            if (clash != null) {
                throw new VerificationException(
                    "NESTED_EXPAND over a dataset cannot follow [{}], which defines [{}]",
                    between.sourceText(),
                    clash
                );
            }
        }
    }

    /**
     * Whether the expansion may move below {@code node}: it computes each row from that row alone. A limit below the
     * expansion is the copy {@code PushDownAndCombineLimits} puts there, which the scan turns into its parent limit.
     * This is the one list of what may sit under the expansion: {@code PushNestedExpandToExternalSource} rebuilds
     * whatever it allows structurally, with no list of its own.
     */
    private static boolean pushesThrough(LogicalPlan node) {
        if (node instanceof Eval eval) {
            return deterministic(eval.fields());
        }
        if (node instanceof Filter filter) {
            return deterministic(List.of(filter.condition()));
        }
        return node instanceof Project
            || node instanceof Dissect
            || node instanceof Grok
            || node instanceof CompoundOutputEval<?>
            || node instanceof Enrich
            || node instanceof LookupJoin
            || node instanceof MvExpand
            || node instanceof Limit;
    }

    /** Whether no expression in {@code expressions} gives a different value each time it is evaluated. */
    public static boolean deterministic(List<? extends Expression> expressions) {
        for (Expression expression : expressions) {
            if (expression.anyMatch(e -> e instanceof NonDeterministic)) {
                return false;
            }
        }
        return true;
    }

    /** Whether {@code attributes} read the record reference, which addresses a file row and so no element. */
    public static boolean readsRecordRef(List<Attribute> attributes) {
        for (Attribute attribute : attributes) {
            if (FileMetadataColumns.RECORD_REF.equals(attribute.name())) {
                return true;
            }
        }
        return false;
    }

    /**
     * The declared rename to or from {@code path}, as {@code logical -> physical}, or {@code null}. The expansion's path is
     * the collection's name in the files, which a rename does not reach, so a rename involving it would make the dataset's
     * column and the collection two things under one name.
     */
    @Nullable
    public static Map.Entry<String, String> renameOf(DeclaredReadSpec spec, String path) {
        for (Map.Entry<String, String> rename : spec.renames().entrySet()) {
            if (rename.getKey().equals(path) || rename.getValue().equals(path)) {
                return rename;
            }
        }
        return null;
    }

    /**
     * The first of {@code attributes} named like an expanded column, or {@code null}. Moved below the node defining it, the
     * expanded column would be shadowed by it, where above it the expanded column shadows it.
     */
    @Nullable
    public static String clashingName(List<Attribute> attributes, Set<String> expandedNames) {
        for (Attribute attribute : attributes) {
            if (expandedNames.contains(attribute.name())) {
                return attribute.name();
            }
        }
        return null;
    }

    /** The names of {@code attributes}. */
    public static Set<String> names(List<Attribute> attributes) {
        Set<String> names = new HashSet<>();
        for (Attribute attribute : attributes) {
            names.add(attribute.name());
        }
        return names;
    }
}
