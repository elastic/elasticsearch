/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.function.scalar.BinaryScalarFunction;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.tree.Source;

import java.time.ZoneId;
import java.util.Objects;

import static org.elasticsearch.xpack.ql.expression.gen.script.ParamsBuilder.paramsBuilder;

public abstract class BinaryDateTimeFunction extends BinaryScalarFunction {

    private final ZoneId zoneId;

    public BinaryDateTimeFunction(Source source, Expression datePart, Expression timestamp, ZoneId zoneId) {
        super(source, datePart, timestamp);
        this.zoneId = zoneId;
    }

    @Override
    protected abstract TypeResolution resolveType();

    public ZoneId zoneId() {
        return zoneId;
    }

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
        if (super.equals(o) == false) {
            return false;
        }
        BinaryDateTimeFunction that = (BinaryDateTimeFunction) o;
        return zoneId.equals(that.zoneId);
    }
}
