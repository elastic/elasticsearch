/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.function.scalar.BinaryScalarFunction;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.tree.Source;

import java.time.ZoneId;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isString;
import static org.elasticsearch.xpack.ql.expression.gen.script.ParamsBuilder.paramsBuilder;
import static org.elasticsearch.xpack.sql.expression.SqlTypeResolutions.isDate;

public abstract class BinaryDateTimeFunction extends BinaryScalarFunction {

    private final ZoneId zoneId;

    public BinaryDateTimeFunction(Source source, Expression datePart, Expression timestamp, ZoneId zoneId) {
        super(source, datePart, timestamp);
        this.zoneId = zoneId;
    }

    @Override
    protected TypeResolution resolveType() {
        TypeResolution resolution = isString(left(), sourceText(), Expressions.ParamOrdinal.FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }

        if (left().foldable()) {
            String datePartValue = (String) left().fold();
            if (datePartValue != null && resolveDateTimeField(datePartValue) == false) {
                List<String> similar = findSimilarDateTimeFields(datePartValue);
                if (similar.isEmpty()) {
                    return new TypeResolution(format(null, "first argument of [{}] must be one of {} or their aliases; found value [{}]",
                        sourceText(),
                        validDateTimeFieldValues(),
                        Expressions.name(left())));
                } else {
                    return new TypeResolution(format(null, "Unknown value [{}] for first argument of [{}]; did you mean {}?",
                        Expressions.name(left()),
                        sourceText(),
                        similar));
                }
            }
        }
        resolution = isDate(right(), sourceText(), Expressions.ParamOrdinal.SECOND);
        if (resolution.unresolved()) {
            return resolution;
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    public ZoneId zoneId() {
        return zoneId;
    }

    protected abstract boolean resolveDateTimeField(String dateTimeField);

    protected abstract List<String> findSimilarDateTimeFields(String dateTimeField);

    protected abstract List<String> validDateTimeFieldValues();

    @Override
    protected Pipe makePipe() {
        return createPipe(Expressions.pipe(left()), Expressions.pipe(right()), zoneId);
    }

    protected abstract Pipe createPipe(Pipe left, Pipe right, ZoneId zoneId);

    @Override
    protected ScriptTemplate asScriptFrom(ScriptTemplate leftScript, ScriptTemplate rightScript) {
        return new ScriptTemplate(
            formatTemplate("{sql}." + scriptMethodName() +
                "(" + leftScript.template() + "," + rightScript.template()+ ",{})"),
            paramsBuilder()
                .script(leftScript.params())
                .script(rightScript.params())
                .variable(zoneId.getId())
                .build(),
            dataType());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), zoneId);
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
        BinaryDateTimeFunction that = (BinaryDateTimeFunction) o;
        return zoneId.equals(that.zoneId);
    }
}
