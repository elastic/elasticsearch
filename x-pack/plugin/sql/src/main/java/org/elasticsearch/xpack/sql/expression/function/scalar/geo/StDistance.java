/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.geo;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.expression.predicate.BinaryOperator;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import static org.elasticsearch.xpack.ql.expression.gen.script.ParamsBuilder.paramsBuilder;
import static org.elasticsearch.xpack.sql.expression.SqlTypeResolutions.isGeo;

/**
 * Calculates the distance between two points
 */
public class StDistance extends BinaryOperator<Object, Object, Double, StDistanceFunction> {

    private static final StDistanceFunction FUNCTION = new StDistanceFunction();

    public StDistance(Source source, Expression source1, Expression source2) {
        super(source, source1, source2, FUNCTION);
    }

    @Override
    protected StDistance replaceChildren(Expression newLeft, Expression newRight) {
        return new StDistance(source(), newLeft, newRight);
    }

    @Override
    public DataType dataType() {
        return DataTypes.DOUBLE;
    }

    @Override
    protected NodeInfo<StDistance> info() {
        return NodeInfo.create(this, StDistance::new, left(), right());
    }

    @Override
    public ScriptTemplate scriptWithField(FieldAttribute field) {
        return new ScriptTemplate(processScript("{sql}.geoDocValue(doc,{})"),
            paramsBuilder().variable(field.exactAttribute().name()).build(),
            dataType());
    }

    @Override
    protected TypeResolution resolveInputType(Expression e, ParamOrdinal paramOrdinal) {
        return isGeo(e, sourceText(), paramOrdinal);
    }

    @Override
    public StDistance swapLeftAndRight() {
        return new StDistance(source(), right(), left());
    }

    @Override
    protected Pipe makePipe() {
        return new StDistancePipe(source(), this, Expressions.pipe(left()), Expressions.pipe(right()));
    }

    @Override
    protected String scriptMethodName() {
        return "stDistance";
    }
}
