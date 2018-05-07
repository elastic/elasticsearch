/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.FieldAttribute;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeProcessor.DateTimeExtractor;
import org.elasticsearch.xpack.sql.expression.function.scalar.script.ParamsBuilder;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo.NodeCtor2;
import org.elasticsearch.xpack.sql.type.DataType;

import java.time.temporal.ChronoField;
import java.util.TimeZone;

import static org.elasticsearch.xpack.sql.expression.function.scalar.script.ScriptTemplate.formatTemplate;

/**
 * Extract the day name from a datetime.
 * <pre>
 * DATENAME("2017-12-05T00:00:00")
 * returns "Tuesday"
 * </pre>
 *
 */
public class DayName extends DateTimeFunction {

    // DateTimeFormatter.ofPattern
    static final String FORMAT = "EEEE";

    public DayName(Location location, Expression field, TimeZone timeZone) {
        super(location, field, timeZone);
    }

    @Override
    protected NodeCtor2<Expression, TimeZone, DateTimeFunction> ctorForInfo() {
        return DayName::new;
    }

    @Override
    protected DayName replaceChild(Expression newChild) {
        return new DayName(location(), newChild, timeZone());
    }

    @Override
    public String dateTimeFormat() {
        return FORMAT;
    }

    @Override
    public DataType dataType() {
        return DataType.TEXT;
    }

    @Override
    protected ChronoField chronoField() {
        return ChronoField.DAY_OF_WEEK;
    }

    @Override
    protected DateTimeExtractor extractor() {
        return DateTimeExtractor.DAYNAME;
    }

    // overridden because original fetches field and returns a number, while this needs to format the datetime.
    @Override
    String scriptTemplateTimezone(final FieldAttribute field, final ParamsBuilder params) {
        params.variable(field.name()) // doc $param 1
            .variable(this.timeZone().getID()) // ZoneId.of $param 2
            .variable(FORMAT); // ofPattern $param 3

        return formatTemplate("ZonedDateTime.ofInstant(Instant.ofEpochMilli(doc[{}].value.millis), " +
            "ZoneId.of({}).format(DateTimeFormatter.ofPattern({}))");
    }
}
