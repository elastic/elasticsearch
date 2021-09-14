/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.expression.predicate.nulls;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Nullability;
import org.elasticsearch.xpack.ql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.ql.expression.gen.script.Scripts;
import org.elasticsearch.xpack.ql.expression.predicate.Negatable;
import org.elasticsearch.xpack.ql.expression.predicate.nulls.CheckNullProcessor.CheckNullOperation;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

public class IsNotNull extends UnaryScalarFunction implements Negatable<UnaryScalarFunction> {

    public IsNotNull(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected NodeInfo<IsNotNull> info() {
        return NodeInfo.create(this, IsNotNull::new, field());
    }

    @Override
    protected IsNotNull replaceChild(Expression newChild) {
        return new IsNotNull(source(), newChild);
    }

    @Override
    public Object fold() {
        return field().fold() != null && DataTypes.isNull(field().dataType()) == false;
    }

    @Override
    protected Processor makeProcessor() {
        return new CheckNullProcessor(CheckNullOperation.IS_NOT_NULL);
    }

    @Override
    public String processScript(String script) {
        return Scripts.formatTemplate(Scripts.QL_SCRIPTS + ".isNotNull(" + script + ")");
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
        return new IsNull(source(), field());
    }
}
