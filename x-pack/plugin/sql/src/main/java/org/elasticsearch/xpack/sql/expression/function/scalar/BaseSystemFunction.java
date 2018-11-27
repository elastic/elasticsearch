/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar;

import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.session.Configuration;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.util.StringUtils;

import java.util.List;

public abstract class BaseSystemFunction extends ScalarFunction {

    private final Configuration configuration;

    public BaseSystemFunction(Location location, Configuration configuration) {
        super(location);
        this.configuration = configuration;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        throw new UnsupportedOperationException("this node doesn't have any children");
    }

    @Override
    public DataType dataType() {
        return DataType.KEYWORD;
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
        throw new SqlIllegalArgumentException("System functions cannot be scripted");
    }
    
    protected Configuration configuration() {
        return configuration;
    }
}