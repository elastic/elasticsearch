/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Nullability;
import org.elasticsearch.xpack.ql.expression.function.scalar.ConfigurationFunction;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.session.Configuration;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import java.util.List;
import java.util.Objects;

import static java.util.Collections.emptyList;

public abstract class SqlConfigurationFunction extends ConfigurationFunction {

    private final DataType dataType;

    protected SqlConfigurationFunction(Source source, Configuration configuration, DataType dataType) {
        super(source, emptyList(), configuration);
        this.dataType = dataType;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        throw new UnsupportedOperationException("this node doesn't have any children");
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
        return super.equals(obj) && Objects.equals(fold(), ((SqlConfigurationFunction) obj).fold());
    }
}
