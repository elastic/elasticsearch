/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate.nulls;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Nullability;
import org.elasticsearch.xpack.ql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.ql.expression.gen.script.Scripts;
import org.elasticsearch.xpack.ql.expression.predicate.Negatable;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.sql.expression.predicate.nulls.CheckNullProcessor.CheckNullOperation;

public class IsNull extends UnaryScalarFunction implements Negatable<UnaryScalarFunction> {

    public IsNull(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected NodeInfo<IsNull> info() {
        return NodeInfo.create(this, IsNull::new, field());
    }

    @Override
    protected IsNull replaceChild(Expression newChild) {
        return new IsNull(source(), newChild);
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
    public Nullability nullable() {
        return Nullability.FALSE;
    }

    @Override
    public DataType dataType() {
        return DataTypes.BOOLEAN;
    }

    @Override
    public UnaryScalarFunction negate() {
        return new IsNotNull(source(), field());
    }
}
