/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.qlcore.expression.predicate.operator.arithmetic;

import org.elasticsearch.xpack.qlcore.expression.Expression;
import org.elasticsearch.xpack.qlcore.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.qlcore.expression.gen.processor.Processor;
import org.elasticsearch.xpack.qlcore.expression.gen.script.Scripts;
import org.elasticsearch.xpack.qlcore.expression.predicate.operator.arithmetic.UnaryArithmeticProcessor.UnaryArithmeticOperation;
import org.elasticsearch.xpack.qlcore.tree.NodeInfo;
import org.elasticsearch.xpack.qlcore.tree.Source;
import org.elasticsearch.xpack.qlcore.type.DataType;

import static org.elasticsearch.xpack.qlcore.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.qlcore.expression.TypeResolutions.isNumeric;

/**
 * Negation function (@{code -x}).
 */
public class Neg extends UnaryScalarFunction {

    public Neg(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected NodeInfo<Neg> info() {
        return NodeInfo.create(this, Neg::new, field());
    }

    @Override
    protected Neg replaceChild(Expression newChild) {
        return new Neg(source(), newChild);
    }

    @Override
    protected TypeResolution resolveType() {
        return isNumeric(field(), sourceText(), DEFAULT);
    }

    @Override
    public Object fold() {
        return Arithmetics.negate((Number) field().fold());
    }

    @Override
    public DataType dataType() {
        return field().dataType();
    }

    @Override
    public String processScript(String script) {
        return Scripts.formatTemplate(Scripts.QL_SCRIPTS + ".neg(" + script + ")");
    }

    @Override
    protected Processor makeProcessor() {
        return new UnaryArithmeticProcessor(UnaryArithmeticOperation.NEGATE);
    }
}
