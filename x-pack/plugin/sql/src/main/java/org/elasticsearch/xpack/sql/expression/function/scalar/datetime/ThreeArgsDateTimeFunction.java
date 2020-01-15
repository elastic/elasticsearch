/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.tree.Source;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static org.elasticsearch.xpack.ql.expression.gen.script.ParamsBuilder.paramsBuilder;

public abstract class ThreeArgsDateTimeFunction extends ScalarFunction {

    private final ZoneId zoneId;

    public ThreeArgsDateTimeFunction(Source source, Expression first, Expression second, Expression third, ZoneId zoneId) {
        super(source, Arrays.asList(first, second, third));
        this.zoneId = zoneId;
    }

    public Expression first() {
        return arguments().get(0);
    }

    public Expression second() {
        return arguments().get(1);
    }

    public Expression third() {
        return arguments().get(2);
    }

    public ZoneId zoneId() {
        return zoneId;
    }

    protected abstract boolean resolveDateTimeField(String dateTimeField);

    protected abstract List<String> findSimilarDateTimeFields(String dateTimeField);

    protected abstract List<String> validDateTimeFieldValues();

    @Override
    public final ThreeArgsDateTimeFunction replaceChildren(List<Expression> newChildren) {
        if (newChildren.size() != 3) {
            throw new IllegalArgumentException("expected [3] children but received [" + newChildren.size() + "]");
        }
        return replaceChildren(newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }

    protected abstract ThreeArgsDateTimeFunction replaceChildren(Expression newFirst, Expression newSecond, Expression newThird);

    @Override
    protected Pipe makePipe() {
        return createPipe(Expressions.pipe(first()), Expressions.pipe(second()), Expressions.pipe(third()), zoneId);
    }

    protected abstract Pipe createPipe(Pipe first, Pipe second, Pipe third, ZoneId zoneId);

    @Override
    public boolean foldable() {
        return first().foldable() && second().foldable() && third().foldable();
    }

    @Override
    public ScriptTemplate asScript() {
        ScriptTemplate firstScript = asScript(first());
        ScriptTemplate secondScript = asScript(second());
        ScriptTemplate thirdScript = asScript(third());

        return asScriptFrom(firstScript, secondScript, thirdScript);
    }

    protected ScriptTemplate asScriptFrom(ScriptTemplate firstScript, ScriptTemplate secondScript, ScriptTemplate thirdScript) {
        return new ScriptTemplate(
            formatTemplate("{sql}." + scriptMethodName() +
                "(" + firstScript.template() + "," + secondScript.template() + "," + thirdScript.template() + ",{})"),
            paramsBuilder()
                .script(firstScript.params())
                .script(secondScript.params())
                .script(thirdScript.params())
                .variable(zoneId.getId())
                .build(),
            dataType());
    }

    protected String scriptMethodName() {
        return getClass().getSimpleName().toLowerCase(Locale.ROOT);
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
        ThreeArgsDateTimeFunction that = (ThreeArgsDateTimeFunction) o;
        return zoneId.equals(that.zoneId);
    }
}
