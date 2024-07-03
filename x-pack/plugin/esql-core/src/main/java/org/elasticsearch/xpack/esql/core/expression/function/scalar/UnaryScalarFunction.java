/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression.function.scalar;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.core.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RegexMatch;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.function.Predicate;

import static java.util.Collections.singletonList;

public abstract class UnaryScalarFunction extends ScalarFunction {

    private final Expression field;

    protected UnaryScalarFunction(Source source, Expression field) {
        super(source, singletonList(field));
        this.field = field;
    }

    protected UnaryScalarFunction(StreamInput in) throws IOException {
        this(Source.readFrom((StreamInput & PlanStreamInput) in), in.readNamedWriteable(Expression.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field);
    }

    @Override
    public final UnaryScalarFunction replaceChildren(List<Expression> newChildren) {
        return replaceChild(newChildren.get(0));
    }

    protected abstract UnaryScalarFunction replaceChild(Expression newChild);

    public Expression field() {
        return field;
    }

    @Override
    public boolean foldable() {
        return field.foldable();
    }

    @Override
    public abstract Object fold();

    @Override
    public boolean canPushToSource(Predicate<FieldAttribute> hasIdenticalDelegate) {
        if (this instanceof RegexMatch<?> || this instanceof IsNull || this instanceof IsNotNull) {
            if (this instanceof IsNull || this instanceof IsNotNull) {
                if (field() instanceof FieldAttribute fa && fa.dataType().equals(DataType.TEXT)) {
                    return true;
                }
            }
            return isAttributePushable(field(), this, hasIdenticalDelegate);
        }
        return false;
    }
}
