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
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.util.StringUtils;

import java.util.List;
import java.util.Objects;

public abstract class ConfigurationFunction extends ScalarFunction {

    private final Configuration configuration;
    private final DataType dataType;

    protected ConfigurationFunction(Location location, Configuration configuration, DataType dataType) {
        super(location);
        this.configuration = configuration;
        this.dataType = dataType;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        throw new UnsupportedOperationException("this node doesn't have any children");
    }

    protected Configuration configuration() {
        return configuration;
    }

    @Override
    public DataType dataType() {
        return dataType;
    }

    @Override
    public boolean nullable() {
        return false;
    }

    @Override
    public boolean foldable() {
        return true;
    }

    @Override
    public abstract Object fold();

    @Override
    protected String functionArgs() {
        return StringUtils.EMPTY;
    }

    @Override
    public ScriptTemplate asScript() {
        return asScript(this);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), fold());
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && Objects.equals(fold(), ((ConfigurationFunction) obj).fold());
    }
}