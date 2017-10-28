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
import org.elasticsearch.xpack.sql.expression.function.aware.TimeZoneAware;
import org.elasticsearch.xpack.sql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeProcessor.DateTimeExtractor;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.ProcessorDefinition;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.ProcessorDefinitions;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.UnaryProcessorDefinition;
import org.elasticsearch.xpack.sql.expression.function.scalar.script.ParamsBuilder;
import org.elasticsearch.xpack.sql.expression.function.scalar.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.DataTypes;
import org.joda.time.DateTimeZone;

import java.time.temporal.ChronoField;

import static org.elasticsearch.xpack.sql.expression.function.scalar.script.ParamsBuilder.paramsBuilder;
import static org.elasticsearch.xpack.sql.expression.function.scalar.script.ScriptTemplate.formatTemplate;

public abstract class DateTimeFunction extends UnaryScalarFunction implements TimeZoneAware {

    private final DateTimeZone timeZone;

    DateTimeFunction(Location location, Expression field, DateTimeZone timeZone) {
        super(location, field);
        this.timeZone = timeZone;
    }
    
    public DateTimeZone timeZone() {
        return timeZone;
    }

    public boolean foldable() {
        return field().foldable();
    }

    @Override
    protected TypeResolution resolveType() {
        return field().dataType().same(DataTypes.DATE) ? 
                    TypeResolution.TYPE_RESOLVED :
                    new TypeResolution("Function '%s' cannot be applied on a non-date expression ('%s' of type '%s')", functionName(), Expressions.name(field()), field().dataType().esName());
    }

    @Override
    public ScriptTemplate asScript() {
        return super.asScript();
    }

    @Override
    protected ScriptTemplate asScriptFrom(AggregateFunctionAttribute aggregate) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected ScriptTemplate asScriptFrom(FieldAttribute field) {
        ParamsBuilder params = paramsBuilder();
        
        String template = null;
        if (DateTimeZone.UTC.equals(timeZone)) {
            // TODO: it would be nice to be able to externalize the extract function and reuse the script across all extractors
            template = formatTemplate("doc[{}].value.get" + extractFunction() + "()");
            params.variable(field.name());
        } else {
            // TODO ewwww
            /* This uses the Java 8 time API because Painless doesn't whitelist creation of new
             * Joda classes. */

            // ideally JodaTime should be used since that's internally used and there are subtle differences between that and the JDK API
            // all variables are externalized to reuse the script across invocations
            // the actual script is ZonedDateTime.ofInstant(Instant.ofEpochMilli(<insert doc field>.value.millis), ZoneId.of(<insert user tz>)).get(ChronoField.get(MONTH_OF_YEAR))
            
            template = formatTemplate("ZonedDateTime.ofInstant(Instant.ofEpochMilli(doc[{}].value.millis), ZoneId.of({})).get(ChronoField.valueOf({}))");
            params.variable(field.name())
                  .variable(timeZone.getID())
                  .variable(chronoField().name());
        }
        
        return new ScriptTemplate(template, params.build(), dataType());
    }

    @Override
    protected String chainScalarTemplate(String template) {
        throw new UnsupportedOperationException();
    }

    protected String extractFunction() {
        return getClass().getSimpleName();
    }

    /**
     * Used for generating the painless script version of this function when the time zone is not UTC
     */
    protected abstract ChronoField chronoField();

    protected final ProcessorDefinition makeProcessorDefinition() {
        return new UnaryProcessorDefinition(this, ProcessorDefinitions.toProcessorDefinition(field()), new DateTimeProcessor(extractor(), timeZone));
    }

    protected abstract DateTimeExtractor extractor();

    @Override
    public DataType dataType() {
        return DataTypes.INTEGER;
    }

    // used for applying ranges
    public abstract String dateTimeFormat();
}