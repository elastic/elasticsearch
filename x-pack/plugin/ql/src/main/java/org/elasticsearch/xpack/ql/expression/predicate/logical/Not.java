/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.expression.predicate.logical;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions.ParamOrdinal;
import org.elasticsearch.xpack.ql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.ql.expression.gen.script.Scripts;
import org.elasticsearch.xpack.ql.expression.predicate.Negatable;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isBoolean;

public class Not extends UnaryScalarFunction {

    public Not(Source source, Expression child) {
        super(source, child);
    }

    @Override
    protected NodeInfo<Not> info() {
        return NodeInfo.create(this, Not::new, field());
    }

    @Override
    protected Not replaceChild(Expression newChild) {
        return new Not(source(), newChild);
    }

    @Override
    protected TypeResolution resolveType() {
        if (DataType.BOOLEAN == field().dataType()) {
            return TypeResolution.TYPE_RESOLVED;
        }
        return isBoolean(field(), sourceText(), ParamOrdinal.DEFAULT);
    }

    @Override
    public Object fold() {
        return NotProcessor.INSTANCE.process(field().fold());
    }

    @Override
    protected Processor makeProcessor() {
        return NotProcessor.INSTANCE;
    }

    @Override
    public String processScript(String script) {
        return Scripts.formatTemplate(Scripts.SQL_SCRIPTS + ".not(" + script + ")");
    }

    @Override
    protected Expression canonicalize() {
        Expression canonicalChild = field().canonical();
        if (canonicalChild instanceof Negatable) {
            return ((Negatable) canonicalChild).negate();
        }
        return this;
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
    }
}
