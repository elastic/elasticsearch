/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.expression.FieldAttribute;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeProcessor.DateTimeExtractor;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.UnaryPipe;
import org.elasticsearch.xpack.sql.expression.gen.script.ParamsBuilder;
import org.elasticsearch.xpack.sql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;
import org.joda.time.DateTime;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.util.Objects;
import java.util.TimeZone;

import static org.elasticsearch.xpack.sql.expression.gen.script.ParamsBuilder.paramsBuilder;

public abstract class DateTimeFunction extends BaseDateTimeFunction {

    DateTimeFunction(Location location, Expression field, TimeZone timeZone) {
        super(location, field, timeZone);
    }

    @Override
    public Object fold() {
        DateTime folded = (DateTime) field().fold();
        if (folded == null) {
            return null;
        }

        return dateTimeChrono(folded.getMillis(), timeZone().getID(), chronoField().name());
    }

    public static Integer dateTimeChrono(long millis, String tzId, String chronoName) {
        ZonedDateTime time = ZonedDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.of(tzId));
        return Integer.valueOf(time.get(ChronoField.valueOf(chronoName)));
    }

    @Override
    public ScriptTemplate scriptWithField(FieldAttribute field) {
        ParamsBuilder params = paramsBuilder();

        String template = null;
        template = formatTemplate("{sql}.dateTimeChrono(doc[{}].value.millis, {}, {})");
        params.variable(field.name())
              .variable(timeZone().getID())
              .variable(chronoField().name());
        
        return new ScriptTemplate(template, params.build(), dataType());
    }

    /**
     * Used for generating the painless script version of this function when the time zone is not UTC
     */
    protected abstract ChronoField chronoField();

    @Override
    protected Pipe makePipe() {
        return new UnaryPipe(location(), this, Expressions.pipe(field()), new DateTimeProcessor(extractor(), timeZone()));
    }

    protected abstract DateTimeExtractor extractor();

    @Override
    public DataType dataType() {
        return DataType.INTEGER;
    }

    // used for applying ranges
    public abstract String dateTimeFormat();

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        DateTimeFunction other = (DateTimeFunction) obj;
        return Objects.equals(other.field(), field())
            && Objects.equals(other.timeZone(), timeZone());
    }

    @Override
    public int hashCode() {
        return Objects.hash(field(), timeZone());
    }
}