/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.expression;

import org.elasticsearch.xpack.eql.tree.NodeInfo;
import org.elasticsearch.xpack.eql.tree.Source;
import org.elasticsearch.xpack.eql.type.DataType;

import java.util.Objects;

public class Literal extends LeafExpression {

    private final Object value;

    public Literal(Source source, Object value) {
        super(source);
        this.value = value;
    }

    @Override
    public DataType dataType() {
        return DataType.SCALAR;
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Literal::new, value);
    }

    @Override
    public boolean foldable() {
        return true;
    }

    @Override
    public Object fold() {
        return value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Literal other = (Literal) obj;
        return Objects.equals(value, other.value);
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }
}