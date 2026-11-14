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
import java.util.List;
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
        List<UnsupportedAttribute> unionTyped = new ArrayList<>();
        for (Attribute attr : child().output()) {
            // Synthetic attributes (e.g. union-type conversions pushed into UnionAll branches) are dropped before the final output,
            // so the user never groups by them.
            if (attr.synthetic()) {
                continue;
            }
            if (attr instanceof UnsupportedAttribute ua && isUnionType(ua)) {
                unionTyped.add(ua);
            } else {
                Aggregate.checkUnsupportedGroupingType(attr, failures);
            }
        }
        if (unionTyped.isEmpty() == false) {
            String columns = unionTyped.stream()
                .map(ua -> "[" + ua.name() + "] with types " + new TreeSet<>(ua.originalTypes()))
                .collect(Collectors.joining(", "));
            // name() is unquoted. A field such as emp-no has to be quoted or the suggested EVAL does not parse.
            String example = ParserUtils.quoteIdString(unionTyped.getFirst().name());
            failures.add(
                fail(
                    this,
                    "DEDUP cannot compare columns with conflicting types: {}; "
                        + "convert each to a single type first, for example EVAL {} = {}::keyword, or DROP it",
                    columns,
                    example,
                    example
                )
            );
        }
    }

    /**
     * {@code originalTypes()} has one entry per distinct type, not per index, so more than one entry is a type conflict.
     * Types ES|QL already combines, such as {@code date} and {@code date_nanos}, are a single type before this check.
     * Exactly one entry is a type ES|QL does not support.
     */
    private static boolean isUnionType(UnsupportedAttribute attr) {
        return attr.originalTypes().size() > 1;
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
