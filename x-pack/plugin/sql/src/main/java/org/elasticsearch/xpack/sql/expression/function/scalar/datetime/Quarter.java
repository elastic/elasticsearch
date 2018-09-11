/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.FieldAttribute;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.ProcessorDefinition;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.ProcessorDefinitions;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.UnaryProcessorDefinition;
import org.elasticsearch.xpack.sql.expression.function.scalar.script.ParamsBuilder;
import org.elasticsearch.xpack.sql.expression.function.scalar.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo.NodeCtor2;
import org.elasticsearch.xpack.sql.type.DataType;
import org.joda.time.DateTime;

import java.util.Objects;
import java.util.TimeZone;

import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.QuarterProcessor.quarter;
import static org.elasticsearch.xpack.sql.expression.function.scalar.script.ParamsBuilder.paramsBuilder;
import static org.elasticsearch.xpack.sql.expression.function.scalar.script.ScriptTemplate.formatTemplate;

public class Quarter extends BaseDateTimeFunction {

    protected static final String QUARTER_FORMAT = "q";
    
    public Quarter(Location location, Expression field, TimeZone timeZone) {
        super(location, field, timeZone);
    }

    @Override
    public Object fold() {
        DateTime folded = (DateTime) field().fold();
        if (folded == null) {
            return null;
        }

        return quarter(folded.getMillis(), timeZone().getID());
    }

    @Override
    protected ScriptTemplate asScriptFrom(FieldAttribute field) {
        ParamsBuilder params = paramsBuilder();

        String template = null;
        template = formatTemplate("{sql}.quarter(doc[{}].value.millis, {})");
        params.variable(field.name())
              .variable(timeZone().getID());
        
        return new ScriptTemplate(template, params.build(), dataType());
    }

    @Override
    protected NodeCtor2<Expression, TimeZone, BaseDateTimeFunction> ctorForInfo() {
        return Quarter::new;
    }

    @Override
    protected Quarter replaceChild(Expression newChild) {
        return new Quarter(location(), newChild, timeZone());
    }

    @Override
    protected ProcessorDefinition makeProcessorDefinition() {
        return new UnaryProcessorDefinition(location(), this, ProcessorDefinitions.toProcessorDefinition(field()),
                new QuarterProcessor(timeZone()));
    }

    @Override
    public DataType dataType() {
        return DataType.INTEGER;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        BaseDateTimeFunction other = (BaseDateTimeFunction) obj;
        return Objects.equals(other.field(), field())
            && Objects.equals(other.timeZone(), timeZone());
    }

    @Override
    public int hashCode() {
        return Objects.hash(field(), timeZone());
    }

}
