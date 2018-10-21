/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate;

import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.UnaryExpression;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.sql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.DataTypes;

public class IsNotNull extends UnaryExpression {

    public IsNotNull(Location location, Expression child) {
        super(location, child);
    }

    @Override
    protected NodeInfo<IsNotNull> info() {
        return NodeInfo.create(this, IsNotNull::new, child());
    }

    @Override
    protected IsNotNull replaceChild(Expression newChild) {
        return new IsNotNull(location(), newChild);
    }

    @Override
    public Object fold() {
        return child().fold() != null && !DataTypes.isNull(child().dataType());
    }

    @Override
    protected Pipe makePipe() {
        throw new SqlIllegalArgumentException("Not supported yet");
    }

    @Override
    public ScriptTemplate asScript() {
        throw new SqlIllegalArgumentException("Not supported yet");
    }

    @Override
    public boolean nullable() {
        return false;
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
    }

    @Override
    public String toString() {
        return child().toString() + " IS NOT NULL";
    }
}
