/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.session.Configuration;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.util.StringUtils;

import java.util.List;
import java.util.Objects;

public class User extends ScalarFunction {
    
    private final Configuration configuration;

    public User(Location location, Configuration configuration) {
        super(location);
        this.configuration = configuration;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        throw new UnsupportedOperationException("this node doesn't have any children");
    }

    @Override
    protected NodeInfo<User> info() {
        return NodeInfo.create(this, User::new, configuration);
    }

    @Override
    public DataType dataType() {
        return DataType.KEYWORD;
    }

    @Override
    public boolean nullable() {
        return true;
    }

    @Override
    public Object fold() {
        return configuration.username();
    }

    @Override
    public boolean foldable() {
        return true;
    }

    @Override
    protected String functionArgs() {
        return StringUtils.EMPTY;
    }

    @Override
    public ScriptTemplate asScript() {
        return null;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), configuration.username());
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && Objects.equals(configuration.username(), ((User) obj).configuration.username());
    }

}
