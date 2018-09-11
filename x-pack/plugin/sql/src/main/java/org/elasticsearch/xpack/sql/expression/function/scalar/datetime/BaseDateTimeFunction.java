/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.expression.function.aggregate.AggregateFunctionAttribute;
import org.elasticsearch.xpack.sql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.sql.expression.function.scalar.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.type.DataType;

import java.util.TimeZone;

abstract class BaseDateTimeFunction extends UnaryScalarFunction {
    
    private final TimeZone timeZone;
    private final String name;

    BaseDateTimeFunction(Location location, Expression field, TimeZone timeZone) {
        super(location, field);
        this.timeZone = timeZone;

        StringBuilder sb = new StringBuilder(super.name());
        // add timezone as last argument
        sb.insert(sb.length() - 1, " [" + timeZone.getID() + "]");

        this.name = sb.toString();
    }

    @Override
    protected final NodeInfo<BaseDateTimeFunction> info() {
        return NodeInfo.create(this, ctorForInfo(), field(), timeZone());
    }

    protected abstract NodeInfo.NodeCtor2<Expression, TimeZone, BaseDateTimeFunction> ctorForInfo();

    @Override
    protected TypeResolution resolveType() {
        if (field().dataType() == DataType.DATE) {
            return TypeResolution.TYPE_RESOLVED;
        }
        return new TypeResolution("Function [" + functionName() + "] cannot be applied on a non-date expression (["
                + Expressions.name(field()) + "] of type [" + field().dataType().esType + "])");
    }

    public TimeZone timeZone() {
        return timeZone;
    }
    
    @Override
    public String name() {
        return name;
    }

    @Override
    public boolean foldable() {
        return field().foldable();
    }
    
    @Override
    protected ScriptTemplate asScriptFrom(AggregateFunctionAttribute aggregate) {
        throw new UnsupportedOperationException();
    }
}
