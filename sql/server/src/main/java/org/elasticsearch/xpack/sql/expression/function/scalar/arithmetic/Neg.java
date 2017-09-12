/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.arithmetic;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.expression.FieldAttribute;
import org.elasticsearch.xpack.sql.expression.function.aggregate.AggregateFunctionAttribute;
import org.elasticsearch.xpack.sql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.sql.expression.function.scalar.arithmetic.UnaryArithmeticProcessor.UnaryArithmeticOperation;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.ProcessorDefinition;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.ProcessorDefinitions;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.UnaryProcessorDefinition;
import org.elasticsearch.xpack.sql.expression.function.scalar.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;

import static org.elasticsearch.xpack.sql.expression.function.scalar.script.ParamsBuilder.paramsBuilder;
import static org.elasticsearch.xpack.sql.expression.function.scalar.script.ScriptTemplate.formatTemplate;

public class Neg extends UnaryScalarFunction {

    public Neg(Location location, Expression field) {
        super(location, field);
    }

    @Override
    protected TypeResolution resolveType() {
        return Expressions.typeMustBeNumeric(field());
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
    protected ScriptTemplate asScriptFrom(AggregateFunctionAttribute aggregate) {
        return new ScriptTemplate(formatTemplate("{}"),
                paramsBuilder().agg(aggregate.functionId(), aggregate.propertyPath()).build(),
                dataType());
    }

    @Override
    protected ScriptTemplate asScriptFrom(FieldAttribute field) {
        return new ScriptTemplate(formatTemplate("doc[{}].value"),
                paramsBuilder().variable(field.name()).build(),
                dataType());
    }

    @Override
    protected String chainScalarTemplate(String template) {
        return template;
    }

    @Override
    protected ProcessorDefinition makeProcessor() {
        return new UnaryProcessorDefinition(this, ProcessorDefinitions.toProcessorDefinition(field()), new UnaryArithmeticProcessor(UnaryArithmeticOperation.NEGATE));
    }
}
