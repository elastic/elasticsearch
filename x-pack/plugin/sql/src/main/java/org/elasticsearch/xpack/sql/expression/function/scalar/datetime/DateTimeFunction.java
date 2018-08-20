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
import org.elasticsearch.xpack.sql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeProcessor.DateTimeExtractor;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.ProcessorDefinition;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.ProcessorDefinitions;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.UnaryProcessorDefinition;
import org.elasticsearch.xpack.sql.expression.function.scalar.script.ParamsBuilder;
import org.elasticsearch.xpack.sql.expression.function.scalar.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.type.DataType;
import org.joda.time.DateTime;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.util.Objects;
import java.util.TimeZone;

import static org.elasticsearch.xpack.sql.expression.function.scalar.script.ParamsBuilder.paramsBuilder;
import static org.elasticsearch.xpack.sql.expression.function.scalar.script.ScriptTemplate.formatTemplate;

public abstract class DateTimeFunction extends UnaryScalarFunction {

    private final TimeZone timeZone;
    private final String name;

    DateTimeFunction(Location location, Expression field, TimeZone timeZone) {
        super(location, field);
        this.timeZone = timeZone;

        StringBuilder sb = new StringBuilder(super.name());
        // add timezone as last argument
        sb.insert(sb.length() - 1, " [" + timeZone.getID() + "]");

        this.name = sb.toString();
    }

    @Override
    protected final NodeInfo<DateTimeFunction> info() {
        return NodeInfo.create(this, ctorForInfo(), field(), timeZone());
    }

    protected abstract NodeInfo.NodeCtor2<Expression, TimeZone, DateTimeFunction> ctorForInfo();

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
    public boolean foldable() {
        return field().foldable();
    }

    @Override
    public Object fold() {
        DateTime folded = (DateTime) field().fold();
        if (folded == null) {
            return null;
        }

        return dateTimeChrono(folded.getMillis(), timeZone.getID(), chronoField().name());
    }

    public static Integer dateTimeChrono(long millis, String tzId, String chronoName) {
        ZonedDateTime time = ZonedDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.of(tzId));
        return Integer.valueOf(time.get(ChronoField.valueOf(chronoName)));
    }

    @Override
    protected ScriptTemplate asScriptFrom(FieldAttribute field) {
        ParamsBuilder params = paramsBuilder();

        String template = null;
        template = formatTemplate("{sql}.dateTimeChrono(doc[{}].value.millis, {}, {})");
        params.variable(field.name())
              .variable(timeZone.getID())
              .variable(chronoField().name());
        
        return new ScriptTemplate(template, params.build(), dataType());
    }


    @Override
    protected ScriptTemplate asScriptFrom(AggregateFunctionAttribute aggregate) {
        throw new UnsupportedOperationException();
    }

    /**
     * Used for generating the painless script version of this function when the time zone is not UTC
     */
    protected abstract ChronoField chronoField();

    @Override
    protected final ProcessorDefinition makeProcessorDefinition() {
        return new UnaryProcessorDefinition(location(), this, ProcessorDefinitions.toProcessorDefinition(field()),
                new DateTimeProcessor(extractor(), timeZone));
    }

    protected abstract DateTimeExtractor extractor();

    @Override
    public DataType dataType() {
        return DataType.INTEGER;
    }

    // used for applying ranges
    public abstract String dateTimeFormat();

    // add tz along the rest of the params
    @Override
    public String name() {
        return name;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        DateTimeFunction other = (DateTimeFunction) obj;
        return Objects.equals(other.field(), field())
            && Objects.equals(other.timeZone, timeZone);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field(), timeZone);
    }
}