/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.FieldAttribute;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.NamedDateTimeProcessor.NameExtractor;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.ProcessorDefinition;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.ProcessorDefinitions;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.UnaryProcessorDefinition;
import org.elasticsearch.xpack.sql.expression.function.scalar.script.ParamsBuilder;
import org.elasticsearch.xpack.sql.expression.function.scalar.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.util.StringUtils;
import org.joda.time.DateTime;

import java.util.Objects;
import java.util.TimeZone;

import static org.elasticsearch.xpack.sql.expression.function.scalar.script.ParamsBuilder.paramsBuilder;
import static org.elasticsearch.xpack.sql.expression.function.scalar.script.ScriptTemplate.formatTemplate;

public abstract class NamedDateTimeFunction extends BaseDateTimeFunction {

    NamedDateTimeFunction(Location location, Expression field, TimeZone timeZone) {
        super(location, field, timeZone);
    }

    @Override
    public Object fold() {
        DateTime folded = (DateTime) field().fold();
        if (folded == null) {
            return null;
        }

        return extractName(folded.getMillis(), timeZone().getID());
    }

    public abstract String extractName(long millis, String tzId);

    @Override
    protected ScriptTemplate asScriptFrom(FieldAttribute field) {
        ParamsBuilder params = paramsBuilder();

        String template = null;
        template = formatTemplate(formatMethodName("{sql}.{method_name}(doc[{}].value.millis, {})"));
        params.variable(field.name())
              .variable(timeZone().getID());
        
        return new ScriptTemplate(template, params.build(), dataType());
    }
    
    private String formatMethodName(String template) {
        return template.replace("{method_name}", StringUtils.underscoreToLowerCamelCase(nameExtractor().toString()));
    }

    @Override
    protected final ProcessorDefinition makeProcessorDefinition() {
        return new UnaryProcessorDefinition(location(), this, ProcessorDefinitions.toProcessorDefinition(field()),
                new NamedDateTimeProcessor(nameExtractor(), timeZone()));
    }

    protected abstract NameExtractor nameExtractor();
    
    protected abstract String dateTimeFormat();

    @Override
    public DataType dataType() {
        return DataType.KEYWORD;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        NamedDateTimeFunction other = (NamedDateTimeFunction) obj;
        return Objects.equals(other.field(), field())
            && Objects.equals(other.timeZone(), timeZone());
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(field(), timeZone());
    }
}