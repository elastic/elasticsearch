/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate.nulls;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.sql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.sql.expression.gen.script.Scripts;
import org.elasticsearch.xpack.sql.expression.predicate.Negatable;
import org.elasticsearch.xpack.sql.expression.predicate.nulls.CheckNullProcessor.CheckNullOperation;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.DataTypes;

public class IsNull extends UnaryScalarFunction implements Negatable<UnaryScalarFunction> {

    public IsNull(Location location, Expression field) {
        super(location, field);
    }

    @Override
    protected NodeInfo<IsNull> info() {
        return NodeInfo.create(this, IsNull::new, field());
    }

    @Override
    protected IsNull replaceChild(Expression newChild) {
        return new IsNull(location(), newChild);
    }

    @Override
    public Object fold() {
        return field().fold() == null || DataTypes.isNull(field().dataType());
    }

    @Override
    protected Processor makeProcessor() {
        return new CheckNullProcessor(CheckNullOperation.IS_NULL);
    }

    @Override
    public String processScript(String script) {
        return Scripts.formatTemplate(Scripts.SQL_SCRIPTS + ".isNull(" + script + ")");
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
    public UnaryScalarFunction negate() {
        return new IsNotNull(location(), field());
    }
}
