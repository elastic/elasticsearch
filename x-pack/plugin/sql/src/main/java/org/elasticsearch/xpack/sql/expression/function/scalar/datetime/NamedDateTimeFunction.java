/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.FieldAttribute;
import org.elasticsearch.xpack.sql.expression.function.aggregate.AggregateFunctionAttribute;
import org.elasticsearch.xpack.sql.expression.function.grouping.GroupingFunctionAttribute;
import org.elasticsearch.xpack.sql.expression.function.scalar.ScalarFunctionAttribute;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.NamedDateTimeProcessor.NameExtractor;
import org.elasticsearch.xpack.sql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.sql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.util.StringUtils;

import java.time.ZoneId;
import java.util.Locale;

import static java.lang.String.format;
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

    private ScriptTemplate wrapWithNameExtractor(ScriptTemplate nested){
        return new ScriptTemplate(
            formatTemplate(format(Locale.ROOT, "{sql}.%s(%s, {})",
                StringUtils.underscoreToLowerCamelCase(nameExtractor.name()),
                nested.template()
            )),
            paramsBuilder()
                .script(nested.params())
                .variable(zoneId().getId()).build(),
            dataType());
    }

    @Override
    public ScriptTemplate scriptWithFoldable(Expression foldable) {
        ScriptTemplate nested = super.scriptWithFoldable(foldable);

        return wrapWithNameExtractor(nested);
    }

    @Override
    public ScriptTemplate scriptWithScalar(ScalarFunctionAttribute scalar) {
        ScriptTemplate nested = super.scriptWithScalar(scalar);

        return wrapWithNameExtractor(nested);
    }

    @Override
    public ScriptTemplate scriptWithAggregate(AggregateFunctionAttribute aggregate) {
        ScriptTemplate nested = super.scriptWithAggregate(aggregate);

        return wrapWithNameExtractor(nested);
    }

    @Override
    public ScriptTemplate scriptWithGrouping(GroupingFunctionAttribute grouping) {
        ScriptTemplate nested = super.scriptWithGrouping(grouping);

        return wrapWithNameExtractor(nested);
    }

    @Override
    public ScriptTemplate scriptWithField(FieldAttribute field) {
        ScriptTemplate nested = super.scriptWithField(field);

        return wrapWithNameExtractor(nested);
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
