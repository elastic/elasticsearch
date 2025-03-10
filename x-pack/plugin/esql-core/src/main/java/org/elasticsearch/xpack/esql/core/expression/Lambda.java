/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class Lambda extends Expression {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Lambda", Lambda::new);

    private List<? extends Attribute> fields;
    private DataType dataType;

    public Lambda(Source source, List<? extends Attribute> fields, Expression expression) {
        super(source, List.of(expression));
        this.fields = fields;
    }

    public Lambda(StreamInput in) throws IOException {
        super(Source.readFrom((StreamInput & PlanStreamInput) in), List.of(in.readNamedWriteable(Expression.class)));
        this.fields = in.readNamedWriteableCollectionAsList(Attribute.class);
    }

    @Override
    public Nullability nullable() {
        return Nullability.TRUE;
    }

    @Override
    protected TypeResolution resolveType() {
        dataType = condition().dataType();
        return super.resolveType();
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj)
            && Objects.equals(fields, ((Lambda) obj).fields)
            && Objects.equals(condition(), ((Lambda) obj).condition());
    }

    @Override
    public int hashCode() {
        return Objects.hash(fields, condition());
    }

    @Override
    public DataType dataType() {
        return dataType;
    }

    @Override
    public Lambda replaceChildren(List<Expression> newChildren) {
        return new Lambda(source(), fields, newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Lambda::new, fields, children().get(0));
    }

    public Expression condition() {
        return children().get(0);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public List<? extends Attribute> getFields() {
        return fields;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(condition());
        out.writeNamedWriteableCollection(fields);
    }

}
