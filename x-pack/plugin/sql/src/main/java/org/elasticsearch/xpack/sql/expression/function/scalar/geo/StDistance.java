/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.geo;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.expression.FieldAttribute;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.sql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.expression.predicate.BinaryOperator;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.type.DataType;

import static org.elasticsearch.xpack.sql.expression.TypeResolutions.isGeo;
import static org.elasticsearch.xpack.sql.expression.gen.script.ParamsBuilder.paramsBuilder;

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
        return DataType.DOUBLE;
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
    protected TypeResolution resolveInputType(Expression e, Expressions.ParamOrdinal paramOrdinal) {
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
