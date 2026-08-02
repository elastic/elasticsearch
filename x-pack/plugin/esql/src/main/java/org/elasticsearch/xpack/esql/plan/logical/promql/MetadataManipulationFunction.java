/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.promql.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;

/**
 * Dedicated logical node for the PromQL label-manipulation functions {@code label_replace} and {@code label_join}.
 * <p>
 * Both operate purely on a series' labels/identity - never on its sample values - deriving a destination label from one
 * or more source labels and returning an instant vector with the same cardinality as the input. Unlike the other PromQL
 * function families, these are not lowered through the generic function builder: the derivation and the identity rewrite
 * are wired directly during translation (see {@code TranslatePromqlToEsqlPlan}). The {@code parameters()} carried by this
 * node are the keyword-literal arguments after the child instant vector:
 * <ul>
 *     <li>{@code label_replace}: {@code [dst_label, replacement, src_label, regex]}</li>
 *     <li>{@code label_join}: {@code [dst_label, separator, src_label_1, ... src_label_N]}</li>
 * </ul>
 * <p>
 * The derived destination label is a brand-new label that does not exist in the index mapping, so PromQL's global
 * label resolution (which binds label references against the index relation) cannot see it. To let an enclosing
 * {@code by(dst)} or an outer {@code KEEP dst} bind to it exactly like a stored label, the node mints a stable
 * {@link #destination()} attribute once (following the {@code Grok}/{@code Dissect} generated-attribute pattern) and
 * threads it unchanged through the node's lifecycle. Analysis adds that attribute to the PromQL resolution scope (see
 * {@code Analyzer.ResolveAndVerifyPromqlRefs}) so the same identity flows through the aggregate output and the command
 * output contract, and translation materializes the derived column under that same id.
 */
public final class MetadataManipulationFunction extends PromqlFunctionCall {

    private final Attribute destination;

    public MetadataManipulationFunction(
        Source source,
        LogicalPlan child,
        PromqlFunctionDefinition definition,
        List<Expression> parameters
    ) {
        this(source, child, definition, parameters, new ReferenceAttribute(source, destinationName(parameters), DataType.KEYWORD));
    }

    private MetadataManipulationFunction(
        Source source,
        LogicalPlan child,
        PromqlFunctionDefinition definition,
        List<Expression> parameters,
        Attribute destination
    ) {
        super(source, child, definition, parameters);
        this.destination = destination;
    }

    /**
     * The derived destination label as a stable attribute. Its {@code NameId} is minted once and preserved across the
     * node's lifecycle, so the analyzer, the aggregate/command output contract, and the translated column all refer to
     * the one identity - the derived label thus behaves exactly like a stored label end to end.
     */
    public Attribute destination() {
        return destination;
    }

    /** The destination label name, taken from the first keyword-literal argument ({@code dst}). */
    private static String destinationName(List<Expression> parameters) {
        return BytesRefs.toString(((Literal) parameters.get(0)).value());
    }

    @Override
    protected NodeInfo<PromqlFunctionCall> info() {
        return NodeInfo.create(this, MetadataManipulationFunction::new, child(), definition(), parameters(), destination);
    }

    @Override
    public MetadataManipulationFunction replaceChild(LogicalPlan newChild) {
        return new MetadataManipulationFunction(source(), newChild, definition(), parameters(), destination);
    }

    @Override
    public List<Attribute> output() {
        // The destination label is materialized during translation (as an identity-blob rewrite and/or a derived column),
        // so at the logical level the label set is that of the child; the rewrite happens in place on the identity it carries.
        return child().output();
    }

    @Override
    public FunctionType functionType() {
        return FunctionType.METADATA_MANIPULATION;
    }
}
