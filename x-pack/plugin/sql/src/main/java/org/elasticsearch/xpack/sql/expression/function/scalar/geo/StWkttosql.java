/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.geo;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.ql.expression.gen.script.Scripts;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isString;

/**
 * Constructs geometric objects from their WTK representations
 */
public class StWkttosql extends UnaryScalarFunction {

    public StWkttosql(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected StWkttosql replaceChild(Expression newChild) {
        return new StWkttosql(source(), newChild);
    }

    @Override
    protected TypeResolution resolveType() {
        if (field().dataType().isString()) {
            return TypeResolution.TYPE_RESOLVED;
        }
        return isString(field(), sourceText(), Expressions.ParamOrdinal.DEFAULT);
    }

    @Override
    protected Processor makeProcessor() {
        return StWkttosqlProcessor.INSTANCE;
    }

    @Override
    public DataType dataType() {
        return DataType.GEO_SHAPE;
    }

    @Override
    protected NodeInfo<StWkttosql> info() {
        return NodeInfo.create(this, StWkttosql::new, field());
    }

    @Override
    public String processScript(String script) {
        return Scripts.formatTemplate(Scripts.SQL_SCRIPTS + ".stWktToSql(" + script + ")");
    }

    @Override
    public Object fold() {
        return StWkttosqlProcessor.INSTANCE.process(field().fold());
    }

}
