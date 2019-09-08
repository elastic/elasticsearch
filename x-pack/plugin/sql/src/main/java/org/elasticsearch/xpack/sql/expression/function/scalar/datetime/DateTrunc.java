/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.expression.Nullability;
import org.elasticsearch.xpack.sql.expression.function.scalar.BinaryScalarFunction;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.sql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.type.DataType;

import java.time.ZoneId;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.sql.expression.TypeResolutions.isDate;
import static org.elasticsearch.xpack.sql.expression.TypeResolutions.isString;
import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTruncProcessor.process;
import static org.elasticsearch.xpack.sql.expression.gen.script.ParamsBuilder.paramsBuilder;

public class DateTrunc extends BinaryScalarFunction {

    public enum DatePart {

        MILLENNIUM("millennia"),
        CENTURY("centuries"),
        DECADE("decades"),
        YEAR("years", "yy", "yyyy"),
        QUARTER("quarters", "qq", "q"),
        MONTH("months", "mm", "m"),
        WEEK("weeks", "wk", "ww"),
        DAY("days", "dd", "d"),
        HOUR("hours", "hh"),
        MINUTE("minutes", "mi", "n"),
        SECOND("seconds", "ss", "s"),
        MILLISECOND("milliseconds", "ms"),
        MICROSECOND("microseconds", "mcs"),
        NANOSECOND("nanoseconds", "ns");

        private Set<String> aliases;

        DatePart(String... aliases) {
            this.aliases = Set.of(aliases);
        }

        public Set<String> aliases() {
            return aliases;
        }

        public static DatePart resolveTruncate(String truncateTo) {
            for (DatePart datePart : DatePart.values()) {
                truncateTo = truncateTo.toLowerCase(Locale.ROOT);
                if (datePart.name().equalsIgnoreCase(truncateTo) || datePart.aliases().contains(truncateTo)) {
                    return datePart;
                }
            }
            return null;
        }
    }


    private final ZoneId zoneId;

    public DateTrunc(Source source, Expression truncateTo, Expression timestamp, ZoneId zoneId) {
        super(source, truncateTo, timestamp);
        this.zoneId = zoneId;
    }

    @Override
    public DataType dataType() {
        return right().dataType();
    }

    @Override
    protected TypeResolution resolveType() {
        TypeResolution resolution = isString(left(), sourceText(), Expressions.ParamOrdinal.FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }

        if (left().foldable() && DatePart.resolveTruncate((String) left().fold()) == null) {
            return new TypeResolution(format(null, "first argument of [{}] must be one of {} or their aliases, found value [{}]",
                sourceText(),
                DatePart.values(),
                Expressions.name(left())));
        }
        resolution = isDate(right(), sourceText(), Expressions.ParamOrdinal.SECOND);
        if (resolution.unresolved()) {
            return resolution;
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    protected BinaryScalarFunction replaceChildren(Expression newTruncateTo, Expression newTimestamp) {
        return new DateTrunc(source(), newTruncateTo, newTimestamp, zoneId);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, DateTrunc::new, left(), right(), zoneId);
    }

    @Override
    protected Pipe makePipe() {
        return new DateTruncPipe(source(), this, Expressions.pipe(left()), Expressions.pipe(right()), zoneId);
    }

    @Override
    public Nullability nullable() {
        return Nullability.TRUE;
    }

    @Override
    public Object fold() {
        return process(left().fold(), right().fold(), zoneId);
    }

    @Override
    protected ScriptTemplate asScriptFrom(ScriptTemplate leftScript, ScriptTemplate rightScript) {
        return new ScriptTemplate(
            formatTemplate("{sql}.dateTrunc(" + leftScript.template() + "," + rightScript.template()+ ",{})"),
            paramsBuilder()
                .script(leftScript.params())
                .script(rightScript.params())
                .variable(zoneId.getId())
                .build(),
            dataType());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        DateTrunc dateTrunc = (DateTrunc) o;
        return Objects.equals(zoneId, dateTrunc.zoneId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), zoneId);
    }
}
