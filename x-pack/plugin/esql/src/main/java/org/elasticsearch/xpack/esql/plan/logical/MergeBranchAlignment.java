/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.TypeConflictedField;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSUPPORTED;

/**
 * Lines the branches of a {@link MergePlan} up with the merge's output: every branch has to produce the merge's columns, by name
 * and in the merge's order, because the merge concatenates its branches' pages positionally. A branch that lacks a column gets it
 * filled with nulls, and a {@link Project} puts the columns in order.
 * <p>
 * This is the one place that decides how a branch is aligned, so that the optimizer (which materializes the alignment) and anything
 * that needs to see a branch the way it will be aligned (e.g. binding a request filter against a view branch) agree on the null
 * columns' types.
 */
public final class MergeBranchAlignment {

    private MergeBranchAlignment() {}

    /**
     * Aligns every branch of {@code merge} with its output; see {@link #alignBranch}. Returns {@code merge} itself when every branch
     * already lines up.
     */
    public static MergePlan alignBranches(MergePlan merge) {
        List<LogicalPlan> children = merge.children();
        List<LogicalPlan> newChildren = new ArrayList<>(children.size());
        boolean changed = false;
        for (LogicalPlan child : children) {
            LogicalPlan aligned = alignBranch(merge, child);
            changed |= aligned != child;
            newChildren.add(aligned);
        }
        return changed ? merge.replaceSubPlans(newChildren) : merge;
    }

    /**
     * {@code branch} null-filling the columns of {@code merge}'s output that it does not produce, and projected to the merge's
     * columns in the merge's order. Returns {@code branch} itself when it already lines up.
     */
    private static LogicalPlan alignBranch(MergePlan merge, LogicalPlan branch) {
        List<Alias> fills = nullFills(merge.output(), branch);
        LogicalPlan filled = fills.isEmpty() ? branch : new Eval(branch.source(), branch, fills);
        return projectToColumns(filled, Expressions.names(merge.output()));
    }

    /**
     * Null-filling aliases for every column of {@code mergeOutput} that {@code branch} does not output, in merge order; see
     * {@link #nullFill}.
     */
    public static List<Alias> nullFills(List<Attribute> mergeOutput, LogicalPlan branch) {
        Set<String> branchNames = branch.outputSet().names();
        List<Alias> fills = new ArrayList<>();
        for (Attribute column : mergeOutput) {
            if (branchNames.contains(column.name()) == false) {
                fills.add(nullFill(branch.source(), column));
            }
        }
        return fills;
    }

    /**
     * The columns {@code branch} contributes to a merge with output {@code mergeOutput}, in the merge's order: the branch's own
     * attribute for each merge column it produces and, for one it lacks, the attribute of the matching fill in {@code nullFills} (see
     * {@link #nullFills}). A column the branch produces that is not a merge column does not reach the merge, so it is not included.
     * <p>
     * This is the branch as {@link #alignBranch} aligns it, for callers that need to see it that way before it is materialized.
     */
    public static List<Attribute> alignedColumns(List<Attribute> mergeOutput, LogicalPlan branch, List<Alias> nullFills) {
        Map<String, Attribute> byName = attributesByName(branch.output());
        for (Alias fill : nullFills) {
            Attribute previous = byName.put(fill.name(), fill.toAttribute());
            assert previous == null : "null fill [" + fill.name() + "] for a column the merge branch already outputs " + branch.output();
        }
        List<Attribute> columns = new ArrayList<>(mergeOutput.size());
        for (Attribute column : mergeOutput) {
            columns.add(byName.get(column.name()));
        }
        return columns;
    }

    /** {@link #alignedColumns(List, LogicalPlan, List)} with the fills {@link #nullFills} creates for the branch. */
    public static List<Attribute> alignedColumns(List<Attribute> mergeOutput, LogicalPlan branch) {
        return alignedColumns(mergeOutput, branch, nullFills(mergeOutput, branch));
    }

    /**
     * An alias filling {@code column} with nulls in a branch that lacks it, with the branch's {@code source}, typed
     * {@link #nullFillType}.
     * <p>
     * Preserves {@link Attribute#synthetic()}, so a fill of a synthetic merge column (e.g. a union-type
     * {@code $$field$converted_to$type} cast) stays invisible to the removal of synthetic attributes from the query's output.
     */
    private static Alias nullFill(Source source, Attribute column) {
        return new Alias(source, column.name(), new Literal(source, null, nullFillType(column)), null, column.synthetic());
    }

    /**
     * The type of the nulls filling {@code column} in a branch that lacks it: {@link #alignmentDataType} of the column, except for
     * types that an {@code EVAL} cannot return. {@code UNSUPPORTED} is filled as {@code KEYWORD} and a counter type as its
     * non-counter counterpart.
     */
    private static DataType nullFillType(Attribute column) {
        DataType type = alignmentDataType(column);
        type = type == UNSUPPORTED ? KEYWORD : type;
        return type.isCounter() ? type.noCounter() : type;
    }

    /**
     * The effective data type of a branch or output attribute when aligning the branches of a {@link MergePlan}: a two-legged PUNK
     * (mapped with a single type in some indices, unmapped in the others) is aligned as its widened mapped type.
     */
    public static DataType alignmentDataType(Attribute attr) {
        if (attr instanceof FieldAttribute fa && fa.field() instanceof TypeConflictedField tcf && tcf.isSingleTypePotentiallyUnmapped()) {
            return tcf.singleMappedTypeWidened();
        }
        return attr.dataType();
    }

    /**
     * {@code branch} under a {@link Project} over {@code mergeColumns}, in that order, unless it already is one.
     * <p>
     * Union alignment is structural, not user-named: this emits a plain {@link Project} rather than a {@code Keep}. A {@code Keep}
     * would falsely register every virtual attribute in the alignment projection (e.g. EXTERNAL's shim-injected {@code _file.*}
     * family) as "the user explicitly KEEP'd it", which {@code planWithoutSyntheticAttributes} then refuses to strip, leaking the
     * columns to the output. A user-named {@code KEEP _file.path} upstream of the union still survives via its own {@code Keep}
     * node in the branch's plan.
     */
    private static LogicalPlan projectToColumns(LogicalPlan branch, List<String> mergeColumns) {
        if (branch instanceof Project && Expressions.names(branch.output()).equals(mergeColumns)) {
            return branch;
        }
        if (mergeColumns.isEmpty()) {
            // When there are no merge columns (every branch only has no-fields), project to nothing so the no-fields marker
            // doesn't leak into the branch output.
            return new Project(branch.source(), branch, List.of());
        }
        return new Project(branch.source(), branch, alignToMergeColumns(mergeColumns, branch.output()));
    }

    /**
     * Picks the attributes of a merge branch's output in {@code mergeColumns} order. The branch output is indexed by name once
     * so the cost is linear in the schema width: a merge over thousands of columns would otherwise rescan the branch output for
     * every merge column, saturating the threads that plan queries (#163560).
     */
    static List<Attribute> alignToMergeColumns(List<String> mergeColumns, List<Attribute> branchOutput) {
        Map<String, Attribute> branchAttributesByName = attributesByName(branchOutput);
        List<Attribute> aligned = new ArrayList<>(mergeColumns.size());
        for (String name : mergeColumns) {
            Attribute match = branchAttributesByName.get(name);
            if (match != null) {
                aligned.add(match);
            }
        }
        return aligned;
    }

    /**
     * A merge branch's output indexed by name, so that looking up every merge column is linear in the schema width. A plan's output
     * never repeats a name, so each merge column matches at most one branch attribute.
     */
    private static Map<String, Attribute> attributesByName(List<Attribute> branchOutput) {
        Map<String, Attribute> byName = HashMap.newHashMap(branchOutput.size());
        for (Attribute attr : branchOutput) {
            Attribute previous = byName.put(attr.name(), attr);
            assert previous == null : "duplicate attribute name [" + attr.name() + "] in merge branch output " + branchOutput;
        }
        return byName;
    }
}
