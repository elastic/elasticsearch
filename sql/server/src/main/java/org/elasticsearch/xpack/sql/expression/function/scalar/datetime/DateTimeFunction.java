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
import org.elasticsearch.xpack.sql.expression.function.scalar.ColumnProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.sql.expression.function.scalar.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.DataTypes;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.ReadableDateTime;

import java.time.temporal.ChronoField;
import java.util.TimeZone;

import static org.elasticsearch.xpack.sql.expression.function.scalar.script.ParamsBuilder.paramsBuilder;
import static org.elasticsearch.xpack.sql.expression.function.scalar.script.ScriptTemplate.formatTemplate;

public abstract class DateTimeFunction extends ScalarFunction {
    private final TimeZone timeZone;

    // NOCOMMIT I feel like our lives could be made a lot simpler with composition instead of inheritance here
    public DateTimeFunction(Location location, Expression argument, TimeZone timeZone) {
        super(location, argument);
        this.timeZone = timeZone;
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
        // NOCOMMIT I think we should investigate registering SQL as a script engine so we don't need to generate painless
        return new ScriptTemplate(createTemplate(), 
                paramsBuilder()
                    .variable(field.name())
                    .build(),
                dataType());
    }

    @Override
    protected String chainScalarTemplate(String template) {
        throw new UnsupportedOperationException();
    }

    private String createTemplate() {
        if (timeZone.getID().equals("UTC")) {
            return formatTemplate("doc[{}].value.get" + extractFunction() + "()");
        } else {
            // NOCOMMIT ewwww
            /* This uses the Java 9 time API because Painless doesn't whitelist creation of new
             * Joda classes. */
            String asInstant = formatTemplate("Instant.ofEpochMilli(doc[{}].value.millis)");
            String zoneId = "ZoneId.of(\"" + timeZone.toZoneId().getId() + "\"";
            String asZonedDateTime = "ZonedDateTime.ofInstant(" + asInstant + ", " + zoneId + "))";
            return asZonedDateTime + ".get(ChronoField." + chronoField().name() + ")";
        }
    }

    protected String extractFunction() {
        return getClass().getSimpleName();
    }

    @Override
    public ColumnProcessor asProcessor() {
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
            if (false == timeZone.getID().equals("UTC")) {
                // TODO probably faster to use `null` for UTC like core does
                dt = dt.toDateTime().withZone(DateTimeZone.forTimeZone(timeZone));
            }
            return Integer.valueOf(extract(dt));
        };
    }

    @Override
    public DataType dataType() {
        return DataTypes.INTEGER;
    }

    public TimeZone timeZone() {
        return timeZone;
    }

    protected abstract int extract(ReadableDateTime dt);

    // used for aggregration (date histogram)
    public abstract String interval();

    // used for applying ranges
    public abstract String dateTimeFormat();

    // used for generating the painless script version of this function when the time zone is not utc
    protected abstract ChronoField chronoField();
}
