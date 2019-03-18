/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Nullability;
import org.elasticsearch.xpack.sql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.session.Configuration;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.type.DataType;

import java.util.List;
import java.util.Objects;

public abstract class ConfigurationFunction extends ScalarFunction {

    private final Configuration configuration;
    private final DataType dataType;

    protected ConfigurationFunction(Source source, Configuration configuration, DataType dataType) {
        super(source);
        this.configuration = configuration;
        this.dataType = dataType;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        throw new UnsupportedOperationException("this node doesn't have any children");
    }

    public Configuration configuration() {
        return configuration;
    }

    @Override
    public DataType dataType() {
        return dataType;
    }

    @Override
    public Nullability nullable() {
        return Nullability.FALSE;
    }

    @Override
    public boolean foldable() {
        return true;
    }

    @Override
    public abstract Object fold();

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
