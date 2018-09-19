/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.math;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Literal;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.BinaryMathProcessor.BinaryMathOperation;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.ProcessorDefinition;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.ProcessorDefinitions;
import org.elasticsearch.xpack.sql.expression.function.scalar.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.type.DataType;

import java.util.Locale;
import java.util.function.BiFunction;

import static java.lang.String.format;
import static org.elasticsearch.xpack.sql.expression.function.scalar.script.ParamsBuilder.paramsBuilder;

/**
 * Function that takes two parameters: one is the field/value itself, the other is a non-floating point numeric
 * which indicates how the rounding should behave. If positive, it will round the number till that parameter
 * count digits after the decimal point. If negative, it will round the number till that paramter count
 * digits before the decimal point, starting at the decimal point.
 */
public class Round extends BinaryNumericFunction {
    
    public Round(Location location, Expression left, Expression right) {
        super(location, left, right == null ? Literal.of(Location.EMPTY, 0) : right);
    }

    @Override
    protected NodeInfo<Round> info() {
        return NodeInfo.create(this, Round::new, left(), right());
    }

    @Override
    protected Round replaceChildren(Expression newLeft, Expression newRight) {
        return new Round(location(), newLeft, newRight);
    }

    @Override
    protected BiFunction<Number, Number, Number> operation() {
        return BinaryMathOperation.ROUND;
    }

    @Override
    protected ProcessorDefinition makeProcessorDefinition() {
        return new BinaryMathProcessorDefinition(location(), this,
                ProcessorDefinitions.toProcessorDefinition(left()),
                ProcessorDefinitions.toProcessorDefinition(right()),
                BinaryMathOperation.ROUND);
    }
    
    protected TypeResolution resolveInputType(DataType inputType) {
        return inputType.isNumeric() ? 
                TypeResolution.TYPE_RESOLVED : 
                new TypeResolution("'%s' requires a numeric type, received %s", mathFunction(), inputType.esType);
    }

    @Override
    protected ScriptTemplate asScriptFrom(ScriptTemplate leftScript, ScriptTemplate rightScript) {
        return new ScriptTemplate(format(Locale.ROOT, ScriptTemplate.formatTemplate("{sql}.%s(%s,%s)"), 
                mathFunction(), 
                leftScript.template(), 
                rightScript.template()),
                paramsBuilder()
                    .script(leftScript.params()).script(rightScript.params())
                    .build(), dataType());
    }
    
    @Override
    public DataType dataType() {
        return left().dataType();
    }
}
