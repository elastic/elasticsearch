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
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.type.DataType.UNSUPPORTED;

/**
 * A lambda expression, e.g. {@code x -> x + 1} or {@code (x, y) -> x + y}, as accepted by functions
 * that take a lambda argument (e.g. {@code map(field, x -> to_upper(x))}).
 * <p>
 * Parameters are modeled as the leading children, with the lambda body as the last child, so that
 * {@link #replaceChildren} and equality/serialization fall out of the standard {@link org.elasticsearch.xpack.esql.core.tree.Node}
 * contract without any extra non-child state to keep in sync.
 */
public class Lambda extends Expression {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Lambda", Lambda::new);

    public Lambda(Source source, List<Expression> parametersAndBody) {
        super(source, parametersAndBody);
    }

    public Lambda(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteableCollectionAsList(Expression.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteableCollection(children());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @SuppressWarnings("unchecked")
    public List<Attribute> parameters() {
        return (List<Attribute>) (List<?>) children().subList(0, children().size() - 1);
    }

    public Expression body() {
        return children().get(children().size() - 1);
    }

    @Override
    public Lambda replaceChildren(List<Expression> newChildren) {
        return new Lambda(source(), newChildren);
    }

    @Override
    protected NodeInfo<Lambda> info() {
        return NodeInfo.create(this, Lambda::new, children());
    }

    @Override
    public Nullability nullable() {
        return Nullability.FALSE;
    }

    @Override
    public DataType dataType() {
        // No function understands lambdas yet; this placeholder makes function-signature
        // resolution reject a Lambda argument the same way it rejects any other wrong-typed one.
        // TODO: replace with a first-class LAMBDA data type once lambda-aware functions land
        return UNSUPPORTED;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Lambda other = (Lambda) obj;
        return Objects.equals(children(), other.children());
    }

    @Override
    public int hashCode() {
        return Objects.hash(children());
    }

    @Override
    public String toString() {
        String params = parameters().size() == 1
            ? parameters().get(0).toString()
            : "(" + parameters().stream().map(Attribute::toString).reduce((a, b) -> a + ", " + b).orElse("") + ")";
        return params + " -> " + body();
    }
}
