/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar;

import java.util.Objects;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.FieldAttribute;
import org.elasticsearch.xpack.sql.expression.function.aggregate.AggregateFunctionAttribute;
import org.elasticsearch.xpack.sql.expression.function.scalar.script.Params;
import org.elasticsearch.xpack.sql.expression.function.scalar.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.DataTypeConvertion;

import static org.elasticsearch.xpack.sql.expression.function.scalar.script.ParamsBuilder.paramsBuilder;
import static org.elasticsearch.xpack.sql.expression.function.scalar.script.ScriptTemplate.formatTemplate;

public class Cast extends ScalarFunction {

    private final DataType dataType;

    public Cast(Location location, Expression argument, DataType dataType) {
        super(location, argument);
        this.dataType = dataType;
    }

    public DataType from() {
        return argument().dataType();
    }

    public DataType to() {
        return dataType;
    }

    @Override
    public DataType dataType() {
        return dataType;
    }

    @Override
    public boolean nullable() {
        return argument().nullable() || DataTypeConvertion.nullable(from(), to());
    }

    @Override
    protected TypeResolution resolveType() {
        return DataTypeConvertion.canConvert(from(), to()) ? 
            TypeResolution.TYPE_RESOLVED : 
            new TypeResolution("Cannot cast %s to %s", from(), to());
    }


    @Override
    protected ScriptTemplate asScriptFrom(AggregateFunctionAttribute aggregate) {
        // for aggs, there are no params only bucket paths
        Params params = paramsBuilder().agg(aggregate.functionId(), aggregate.propertyPath()).build();
        return new ScriptTemplate(formatTemplate("{}"), params, aggregate.dataType());
    }

    @Override
    protected ScriptTemplate asScriptFrom(ScalarFunctionAttribute scalar) {
        return scalar.script();
    }

    @Override
    protected String chainScalarTemplate(String template) {
        return template;
    }

    @Override
    protected ScriptTemplate asScriptFrom(FieldAttribute field) {
        return new ScriptTemplate(field.name(), Params.EMPTY, field.dataType());
    }

    @Override
    public ColumnProcessor asProcessor() {
        return c -> DataTypeConvertion.convert(c, from(), to());
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && Objects.equals(dataType, ((Cast) obj).dataType());
    }

    @Override
    public String toString() {
        return functionName() + "(" + argument().toString() + " AS " + to().sqlName() + ")#" + id();
    }
}