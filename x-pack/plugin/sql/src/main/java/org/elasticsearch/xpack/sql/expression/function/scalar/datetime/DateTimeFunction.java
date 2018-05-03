/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.common.util.LocaleUtils;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.FunctionContext;
import org.elasticsearch.xpack.sql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeProcessor.DateTimeExtractor;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.ProcessorDefinition;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.ProcessorDefinitions;
import org.elasticsearch.xpack.sql.expression.function.scalar.script.ParamsBuilder;
import org.elasticsearch.xpack.sql.expression.function.scalar.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.type.DataType;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.TimeZone;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.sql.expression.function.scalar.script.ParamsBuilder.paramsBuilder;

abstract public class DateTimeFunction extends ScalarFunction {

    final static TimeZone UTC = TimeZone.getTimeZone("UTC");

    private final FunctionContext context;
    private final String name;

    DateTimeFunction(Location location, List<Expression> arguments, FunctionContext context) {
        super(location, arguments);
        this.context = context;

        StringBuilder sb = new StringBuilder(super.name());
        // add timezone as last argument
        sb.insert(sb.length() - 1, " [" + context + " ]");

        this.name = sb.toString();
    }

    @Override
    protected final NodeInfo<DateTimeFunction> info() {
        return NodeInfo.create(this, ctorForInfo(), arguments(), context());
    }

    abstract NodeInfo.NodeCtor2<List<Expression>, FunctionContext, DateTimeFunction> ctorForInfo();

    public final Expression field() {
        return this.arguments().get(0);
    }

    public final FunctionContext context() {
        return context;
    }

    @Override
    public final Expression replaceChildren(List<Expression> newChildren) {
        return this.replaceChildren(this.location(), newChildren, this.context());
    }

    abstract Expression replaceChildren(Location location, List<Expression> newChildren, FunctionContext context);

    @Override
    public final boolean foldable() {
        return this.arguments().stream()
            .map( e -> e.foldable())
            .allMatch( b -> b);
    }

    @Override
    public final Object fold() {
        // the standard for function is DateTime, String timeZoneId, String locale
        DateTime dateTime = null;
        TimeZone timeZone = context.timeZone();
        Locale locale = context.locale();

        int i = 0;
        for (Expression a : this.arguments()) {
            switch (i) {
                case 0:
                    dateTime = (DateTime) a.fold();
                    break;
                case 1:
                    timeZone = TimeZone.getTimeZone((String) a.fold());
                    break;
                case 2:
                    locale = LocaleUtils.parse((String) a.fold());
                    break;
            }
            i++;
        }

        if (dateTime == null) {
            return null;
        }
        if (!UTC.equals(timeZone)) {
            dateTime = dateTime.toDateTime().withZone(DateTimeZone.forTimeZone(timeZone));
        }

        return this.extractor().extract(dateTime, locale);
    }

    @Override
    final protected TypeResolution resolveType() {
        // this assumes the parameter count check is done in ctor, not here.
        List<DataType> types = this.arguments().stream()
            .map(a -> a.dataType())
            .collect(Collectors.toList());
        List<DataType> expected;

        final int argumentCount = types.size();
        switch (argumentCount) {
            case 1:
                expected = Collections.singletonList(DataType.DATE);
                break;
            case 2:
                expected = Arrays.asList(DataType.DATE, DataType.KEYWORD);
                break;
            case 3:
                expected = Arrays.asList(DataType.DATE, DataType.KEYWORD, DataType.KEYWORD);
                break;
            default:
                throw new IllegalArgumentException("Unexpected argument count " + argumentCount);
        }

        return expected.equals(types) ?
            TypeResolution.TYPE_RESOLVED :
            new TypeResolution("Function [" + functionName() + "] with " +
                argumentCount + " parameters expects ([" + commaSeparated(expected) + "] but got [" +
                commaSeparated(types) + "])");
    }

    private static String commaSeparated(final List<DataType> dataTypes) {
        return dataTypes.stream()
            .map( s -> s.esType)
            .collect(Collectors.joining(", "));
    }

    private String extractFunction() {
        return getClass().getSimpleName();
    }

    @Override
    protected final ProcessorDefinition makeProcessorDefinition() {
        return new DateTimeProcessorDefinition(location(),
            this,
            this.arguments().stream()
                .map(f -> ProcessorDefinitions.toProcessorDefinition(f))
                .collect(Collectors.toList()),
            this.extractor(),
            this.context());
    }

    abstract DateTimeExtractor extractor();

    @Override
    public final ScriptTemplate asScript() {
        ParamsBuilder params = paramsBuilder();

        // doc[{}].value.get$extractFunction({},{})
        StringBuilder template = new StringBuilder();
        template.append("doc[{}].value.get");
        template.append(extractFunction());
        template.append("(");

        // add place holders for each parameter.
        this.arguments().stream()
            .forEach(e -> params.variable(asScript(e)));

        template.append(")");

        return new ScriptTemplate(template.toString(), params.build(), dataType());
    }

    // used for applying ranges
    public abstract String dateTimeFormat();

    // add tz along the rest of the params
    @Override
    final public String name() {
        return name;
    }

    @Override
    final public boolean equals(Object obj) {
        if(obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        DateTimeFunction other = (DateTimeFunction) obj;
        return Objects.equals(other.arguments(), arguments())
            && Objects.equals(other.context(), context());
    }

    @Override
    final public int hashCode() {
        return Objects.hash(arguments(), context());
    }
}
