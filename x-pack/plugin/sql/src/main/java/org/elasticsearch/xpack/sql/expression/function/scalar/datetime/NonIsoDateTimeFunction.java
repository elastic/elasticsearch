/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.FieldAttribute;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.NonIsoDateTimeProcessor.NonIsoDateTimeExtractor;
import org.elasticsearch.xpack.sql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.sql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.util.StringUtils;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Locale;

import static java.lang.String.format;
import static org.elasticsearch.xpack.sql.expression.gen.script.ParamsBuilder.paramsBuilder;

/*
 * Base class for date/time functions that behave differently in a non-ISO format
 */
abstract class NonIsoDateTimeFunction extends BaseDateTimeFunction {

    private final NonIsoDateTimeExtractor extractor;

    NonIsoDateTimeFunction(Source source, Expression field, ZoneId zoneId, NonIsoDateTimeExtractor extractor) {
        super(source, field, zoneId);
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
                  .variable(zoneId().getId()).build(),
                dataType());
    }

    @Override
    protected Processor makeProcessor() {
        return new NonIsoDateTimeProcessor(extractor, zoneId());
    }

    @Override
    public DataType dataType() {
        return DataType.INTEGER;
    }
}