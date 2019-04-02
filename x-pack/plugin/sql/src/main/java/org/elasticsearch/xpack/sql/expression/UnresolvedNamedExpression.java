/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression;

import org.elasticsearch.xpack.sql.capabilities.Unresolvable;
import org.elasticsearch.xpack.sql.capabilities.UnresolvedException;
import org.elasticsearch.xpack.sql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.type.DataType;

import java.util.List;

abstract class UnresolvedNamedExpression extends NamedExpression implements Unresolvable {

    UnresolvedNamedExpression(Source source, List<Expression> children) {
        super(source, "<unresolved>", children, new ExpressionId());
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
    public ExpressionId id() {
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

    @Override
    public ScriptTemplate asScript() {
        throw new UnresolvedException("script", this);
    }
}
