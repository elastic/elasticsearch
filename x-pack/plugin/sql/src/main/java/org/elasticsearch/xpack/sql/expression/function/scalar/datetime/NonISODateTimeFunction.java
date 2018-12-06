/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.FieldAttribute;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.NonISODateTimeProcessor.NonISODateTimeExtractor;
import org.elasticsearch.xpack.sql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.sql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.util.StringUtils;

import java.time.ZonedDateTime;
import java.util.Locale;
import java.util.TimeZone;

import static java.lang.String.format;
import static org.elasticsearch.xpack.sql.expression.gen.script.ParamsBuilder.paramsBuilder;

/*
 * Base class for date/time functions that behave differently in a non-ISO format
 */
abstract class NonISODateTimeFunction extends BaseDateTimeFunction {

    private final NonISODateTimeExtractor extractor;

    NonISODateTimeFunction(Location location, Expression field, TimeZone timeZone, NonISODateTimeExtractor extractor) {
        super(location, field, timeZone);
        this.extractor = extractor;
    }

    @Override
    protected Object doFold(ZonedDateTime dateTime) {
        return extractor.extract(dateTime);
    }

    @Override
    public ScriptTemplate scriptWithField(FieldAttribute field) {
        return new ScriptTemplate(
                formatTemplate(format(Locale.ROOT, "{sql}.%s(doc[{}].value, {})",
                        StringUtils.underscoreToLowerCamelCase(extractor.name()))),
                paramsBuilder()
                  .variable(field.name())
                  .variable(timeZone().getID()).build(),
                dataType());
    }

    @Override
    protected Processor makeProcessor() {
        return new NonISODateTimeProcessor(extractor, timeZone());
    }

    @Override
    public DataType dataType() {
        return DataType.INTEGER;
    }
}