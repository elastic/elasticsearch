/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.expression.FieldAttribute;
import org.elasticsearch.xpack.sql.expression.function.aggregate.AggregateFunctionAttribute;
import org.elasticsearch.xpack.sql.expression.function.scalar.ColumnsProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.sql.expression.function.scalar.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.DataTypes;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.ReadableDateTime;

import java.util.Locale;

import static java.lang.String.format;
import static org.elasticsearch.xpack.sql.expression.function.scalar.script.ParamsBuilder.paramsBuilder;
import static org.elasticsearch.xpack.sql.expression.function.scalar.script.ScriptTemplate.formatTemplate;

public abstract class DateTimeFunction extends ScalarFunction {

    public DateTimeFunction(Location location, Expression argument) {
        super(location, argument);
    }
    
    @Override
    protected TypeResolution resolveType() {
        return argument().dataType().same(DataTypes.DATE) ? 
                    TypeResolution.TYPE_RESOLVED :
                    new TypeResolution("Function '%s' cannot be applied on a non-date expression ('%s' of type '%s')", functionName(), Expressions.name(argument()), argument().dataType().esName());
    }

    @Override
    protected ScriptTemplate asScriptFrom(AggregateFunctionAttribute aggregate) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected ScriptTemplate asScriptFrom(FieldAttribute field) {
        return new ScriptTemplate(createTemplate("doc[{}].date"), 
                paramsBuilder().variable(field.name()).build(),
                dataType());
    }

    @Override
    protected String chainScalarTemplate(String template) {
        throw new UnsupportedOperationException();
    }

    private String createTemplate(String template) {
        return format(Locale.ROOT, "%s.get%s()", formatTemplate(template), extractFunction());
    }

    protected String extractFunction() {
        return getClass().getSimpleName();
    }

    @Override
    public ColumnsProcessor asProcessor() {
        return l -> {
            ReadableDateTime dt = null;
            // most dates are returned as long
            if (l instanceof Long) {
                dt = new DateTime((Long) l, DateTimeZone.UTC);
            }
            // but date histogram returns the keys already as DateTime on UTC
            else {
                dt = (ReadableDateTime) l;
            }
            return Integer.valueOf(extract(dt));
        };
    }

    @Override
    public DataType dataType() {
        return DataTypes.INTEGER;
    }

    protected abstract int extract(ReadableDateTime dt);

    // used for aggregration (date histogram)
    public abstract String interval();

    // used for applying ranges
    public abstract String dateTimeFormat();
}
