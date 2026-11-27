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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
            if (isDedupKey(attr)) {
                groupings.add(attr);
            }
        }
        if (groupings.isEmpty()) {
            return new Limit(source(), one, child());
        }
        return new LimitBy(source(), one, child(), groupings);
    }

    /**
     * The columns DEDUP compares: every column, as if the query listed it in {@code LIMIT 1 BY}. Synthetic attributes
     * (e.g. union-type conversions pushed into UnionAll branches) are dropped before the final output, so the user never
     * dedups on them. A column that is always null cannot tell rows apart and is skipped. The {@code _unmapped_fields}
     * column that {@code LOAD_ALL} adds is a key like any other: rows that differ only in unmapped fields are different
     * rows. {@code _doc} is internal and implicitly dropped.
     */
    private static boolean isDedupKey(Attribute attr) {
        return attr.dataType() != DataType.DOC_DATA_TYPE && attr.synthetic() == false && isAlwaysNull(attr) == false;
    }

    /**
     * An unsupported column that is null in every row: one of a single type, including a type an older node cannot read,
     * or one whose types are all unsupported. A union-type column is not: it holds values of supported types that cannot be
     * compared without a conversion.
     */
    private static boolean isAlwaysNull(Attribute attr) {
        if (attr instanceof UnsupportedAttribute ua) {
            List<String> types = ua.originalTypes();
            return types.stream().distinct().count() <= 1 || types.stream().noneMatch(Dedup::isSupportedType);
        }
        return false;
    }

    /**
     * Original types are Elasticsearch field types for a column read from an index, and ES|QL type names for a column
     * merged from subqueries, so both spellings are looked up.
     */
    private static boolean isSupportedType(String typeName) {
        DataType type = DataType.fromTypeName(typeName);
        if (type == null) {
            type = DataType.fromEs(typeName);
        }
        return type != DataType.UNSUPPORTED;
    }

    @Override
    public void postAnalysisVerification(Failures failures) {
        for (Attribute attr : child().output()) {
            if (isDedupKey(attr) == false) {
                continue;
            }
            if (attr instanceof UnsupportedAttribute unsupported) {
                // A failure is identified by its node, so each column keeps its own failure while reporting at DEDUP.
                failures.add(fail(unsupported.withLocation(source()), "{}", unsupported.unresolvedMessage()));
            } else if (Aggregate.isUnsupportedGrouping(attr)) {
                failures.add(
                    fail(
                        attr,
                        "cannot dedup on [{}] type for column [{}]; convert it to a groupable type or drop it",
                        attr.dataType().typeName(),
                        attr.name()
                    )
                );
            }
        }
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
