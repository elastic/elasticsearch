/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;
import org.elasticsearch.xpack.esql.plan.logical.Dissect;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class DissectExec extends RegexExtractExec {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "DissectExec",
        DissectExec::new
    );

    private final Dissect.Parser parser;

    public DissectExec(
        Source source,
        PhysicalPlan child,
        Expression inputExpression,
        Dissect.Parser parser,
        List<Attribute> extractedAttributes
    ) {
        super(source, child, inputExpression, extractedAttributes);
        this.parser = parser;
    }

    private DissectExec(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            ((PlanStreamInput) in).readPhysicalPlanNode(),
            in.readNamedWriteable(Expression.class),
            Dissect.Parser.readFrom(in),
            in.readNamedWriteableCollectionAsList(Attribute.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        ((PlanStreamOutput) out).writePhysicalPlanNode(child());
        out.writeNamedWriteable(inputExpression());
        parser().writeTo(out);
        out.writeNamedWriteableCollection(extractedFields());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public UnaryExec replaceChild(PhysicalPlan newChild) {
        return new DissectExec(source(), newChild, inputExpression, parser, extractedFields);
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, DissectExec::new, child(), inputExpression, parser, extractedFields);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        DissectExec that = (DissectExec) o;
        return Objects.equals(parser, that.parser);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), parser);
    }

    public Dissect.Parser parser() {
        return parser;
    }
}
