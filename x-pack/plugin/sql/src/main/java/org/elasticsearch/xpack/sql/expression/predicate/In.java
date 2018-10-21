/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate;

import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.NamedExpression;
import org.elasticsearch.xpack.sql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.util.CollectionUtils;

import java.util.List;
import java.util.Objects;

public class In extends NamedExpression {

    private final Expression value;
    private final List<Expression> list;
    private final boolean nullable, foldable;

    public In(Location location, Expression value, List<Expression> list) {
        super(location, null, CollectionUtils.combine(list, value), null);
        this.value = value;
        this.list = list;

        this.nullable = children().stream().anyMatch(Expression::nullable);
        this.foldable = children().stream().allMatch(Expression::foldable);
    }

    @Override
    protected NodeInfo<In> info() {
        return NodeInfo.create(this, In::new, value(), list());
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        if (newChildren.size() < 1) {
            throw new IllegalArgumentException("expected one or more children but received [" + newChildren.size() + "]");
        }
        return new In(location(), newChildren.get(newChildren.size() - 1), newChildren.subList(0, newChildren.size() - 1));
    }

    public Expression value() {
        return value;
    }

    public List<Expression> list() {
        return list;
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
    }

    @Override
    public boolean nullable() {
        return nullable;
    }

    @Override
    public boolean foldable() {
        return foldable;
    }

    @Override
    public Attribute toAttribute() {
        throw new SqlIllegalArgumentException("not implemented yet");
    }

    @Override
    public ScriptTemplate asScript() {
        throw new SqlIllegalArgumentException("not implemented yet");
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, list);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        In other = (In) obj;
        return Objects.equals(value, other.value)
                && Objects.equals(list, other.list);
    }
}