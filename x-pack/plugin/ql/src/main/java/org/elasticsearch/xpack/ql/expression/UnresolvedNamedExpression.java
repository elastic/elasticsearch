/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.expression;

import org.elasticsearch.xpack.ql.capabilities.Unresolvable;
import org.elasticsearch.xpack.ql.capabilities.UnresolvedException;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import java.util.List;

abstract class UnresolvedNamedExpression extends NamedExpression implements Unresolvable {

    UnresolvedNamedExpression(Source source, List<Expression> children) {
        super(source, "<unresolved>", children, new NameId());
    }

    @Override
    public boolean resolved() {
        return false;
    }

    @Override
    public String name() {
        throw new UnresolvedException("name", this);
    }

    @Override
    public NameId id() {
        throw new UnresolvedException("id", this);
    }

    @Override
    public DataType dataType() {
        throw new UnresolvedException("data type", this);
    }

    @Override
    public Attribute toAttribute() {
        throw new UnresolvedException("attribute", this);
    }
}
