/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.NamedDateTimeProcessor.NameExtractor;
import org.elasticsearch.xpack.sql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.sql.expression.gen.script.ParamsBuilder;
import org.elasticsearch.xpack.sql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.util.StringUtils;

import java.time.ZoneId;

import static org.elasticsearch.xpack.sql.expression.gen.script.ParamsBuilder.paramsBuilder;

/*
 * Base class for "named" date/time functions like month_name and day_name
 */
abstract class NamedDateTimeFunction extends BaseDateTimeFunction {

    private final NameExtractor nameExtractor;

    NamedDateTimeFunction(Source source, Expression field, ZoneId zoneId, NameExtractor nameExtractor) {
        super(source, field, zoneId);
        this.nameExtractor = nameExtractor;
    }

    @Override
    public ScriptTemplate asScript() {
        ScriptTemplate script = super.asScript();
        String template = formatTemplate("{sql}." + StringUtils.underscoreToLowerCamelCase(nameExtractor.name())
            + "(" + script.template() + ", {})");
        
        ParamsBuilder params = paramsBuilder().script(script.params()).variable(zoneId().getId());
        
        return new ScriptTemplate(template, params.build(), dataType());
    }

    @Override
    protected Processor makeProcessor() {
        return new NamedDateTimeProcessor(nameExtractor, zoneId());
    }

    @Override
    public DataType dataType() {
        return DataType.KEYWORD;
    }
}
