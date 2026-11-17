/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.UnsupportedAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.parser.ParserUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.common.Failure.fail;

/**
 * Removes duplicate rows from the result set (similar in spirit to SQL {@code DISTINCT}).
 * <p>
 * Implemented as a {@link SurrogateLogicalPlan} that rewrites to
 * {@code LIMIT 1 BY <child.output()>} during the optimizer's substitutions phase.
 * The node itself does not need to escape the coordinator, so it is not registered
 * with the {@code NamedWriteableRegistry}.
 */
public class Dedup extends UnaryPlan implements SurrogateLogicalPlan, TelemetryAware, PostAnalysisVerificationAware, DocPreserving {

    public Dedup(Source source, LogicalPlan child) {
        super(source, child);
    }

    @Override
    public Dedup replaceChild(LogicalPlan newChild) {
        return new Dedup(source(), newChild);
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    @Override
    protected NodeInfo<Dedup> info() {
        return NodeInfo.create(this, Dedup::new, child());
    }

    @Override
    public String telemetryLabel() {
        return "DEDUP";
    }

    @Override
    public LogicalPlan surrogate() {
        Literal one = new Literal(source(), 1, DataType.INTEGER);
        List<Expression> groupings = new ArrayList<>();
        for (Attribute attr : child().output()) {
            // The query never names the column LOAD_ALL adds, so it is not a key; a _unmapped_fields the query names is an ordinary one.
            if (attr.dataType() == DataType.DOC_DATA_TYPE
                || attr instanceof UnsupportedAttribute
                || attr.synthetic()
                || attr instanceof UnmappedFieldsAttribute) {
                continue;
            }
            groupings.add(attr);
        }
        if (groupings.isEmpty()) {
            return new Limit(source(), one, child());
        }
        return new LimitBy(source(), one, child(), groupings);
    }

    @Override
    public void postAnalysisVerification(Failures failures) {
        Map<String, Set<String>> unionTyped = new LinkedHashMap<>();
        for (Attribute attr : child().output()) {
            // Synthetic attributes (e.g. union-type conversions pushed into UnionAll branches) are dropped before the final output,
            // so the user never groups by them.
            if (attr.synthetic()) {
                continue;
            }
            if (attr.dataType() == DataType.UNSUPPORTED) {
                Set<String> types = new TreeSet<>();
                collectConflictTypes(child(), attr, types);
                if (types.size() > 1) {
                    unionTyped.put(attr.name(), types);
                    continue;
                }
            }
            Aggregate.checkUnsupportedGroupingType(attr, failures);
        }
        if (unionTyped.isEmpty() == false) {
            String columns = unionTyped.entrySet()
                .stream()
                .map(e -> "[" + e.getKey() + "] with types " + e.getValue())
                .collect(Collectors.joining(", "));
            List<String> advice = new ArrayList<>();
            // The name is unquoted. A field such as emp-no has to be quoted or the suggested command does not parse.
            unionTyped.entrySet()
                .stream()
                .filter(e -> hasUnsupportedType(e.getValue()) == false)
                .findFirst()
                .map(e -> ParserUtils.quoteIdString(e.getKey()))
                .ifPresent(
                    name -> advice.add(
                        "convert each to a single type first, for example EVAL " + name + " = " + name + "::keyword, or DROP it"
                    )
                );
            // A column whose type is unsupported in some of the data cannot be converted there, so it can only be dropped.
            unionTyped.entrySet()
                .stream()
                .filter(e -> hasUnsupportedType(e.getValue()))
                .findFirst()
                .map(e -> ParserUtils.quoteIdString(e.getKey()))
                .ifPresent(name -> advice.add("drop columns with an unsupported type, for example DROP " + name));
            failures.add(fail(this, "DEDUP cannot compare columns with conflicting types: {}; {}", columns, String.join("; ", advice)));
        }
    }

    /**
     * Collects the types an {@code UNSUPPORTED} column has in the data it comes from, including in every branch of a merge. When
     * every branch has the column as {@code UNSUPPORTED}, the merge keeps the first branch's column, so the merged column's own
     * {@link UnsupportedAttribute#originalTypes()} can miss a union type in a later branch. A column produced from another expression,
     * such as a conversion, is not {@code UNSUPPORTED}, so the walk stops there.
     */
    private static void collectConflictTypes(LogicalPlan plan, Attribute attr, Set<String> types) {
        if (attr.dataType() != DataType.UNSUPPORTED) {
            return;
        }
        if (attr instanceof UnsupportedAttribute ua && isUnionType(ua.originalTypes())) {
            types.addAll(ua.originalTypes());
        } else {
            types.add(DataType.UNSUPPORTED.typeName());
        }
        // Merges match branch columns by name, see MergePlan#outputUnion.
        if (plan instanceof MergePlan merge && merge.outputSet().contains(attr)) {
            for (LogicalPlan branch : merge.children()) {
                for (Attribute branchAttr : branch.output()) {
                    if (branchAttr.name().equals(attr.name())) {
                        collectConflictTypes(branch, branchAttr, types);
                        break;
                    }
                }
            }
            return;
        }
        for (LogicalPlan child : plan.children()) {
            for (Attribute childAttr : child.output()) {
                if (childAttr.id().equals(attr.id())) {
                    collectConflictTypes(child, childAttr, types);
                    return;
                }
            }
        }
    }

    /**
     * Whether a column with these original types has different types in the indices or subqueries a query reads (a union type).
     * A field mapped with conflicting types has one entry per type. A {@code FROM} subquery column has the types of every subquery,
     * so entries can repeat, as in {@code [keyword, keyword, long]}. Types ES|QL already combines, such as {@code date} and
     * {@code date_nanos}, are a single type before this check. A column of a type ES|QL does not support has a single entry.
     * Public for {@code DedupGenerator}, which only has the {@code original_types} of a query response.
     */
    public static boolean isUnionType(List<String> originalTypes) {
        return originalTypes.size() > 1;
    }

    private static boolean hasUnsupportedType(Set<String> types) {
        return types.contains(DataType.UNSUPPORTED.typeName());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("doesn't escape the coordinator node");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("doesn't escape the coordinator node");
    }
}
