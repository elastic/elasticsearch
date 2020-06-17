/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.expression.function.scalar.math;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.Expressions.ParamOrdinal;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.ql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.expression.gen.script.Scripts;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static java.lang.String.format;
import static org.elasticsearch.xpack.eql.expression.function.scalar.math.ToNumberFunctionProcessor.doProcess;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isInteger;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isStringAndExact;
import static org.elasticsearch.xpack.ql.expression.gen.script.ParamsBuilder.paramsBuilder;

/**
 * EQL specific function for parsing strings into numbers.
 */
public class ToNumber extends ScalarFunction implements OptionalArgument {

    private final Expression value, base;

    public ToNumber(Source source, Expression value, Expression base) {
        super(source, Arrays.asList(value, base != null ? base : new Literal(source, null, DataTypes.NULL)));
        this.value = value;
        this.base = arguments().get(1);
    }

    @Override
    protected TypeResolution resolveType() {
        if (!childrenResolved()) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution valueResolution = isStringAndExact(value, sourceText(), ParamOrdinal.FIRST);
        if (valueResolution.unresolved()) {
            return valueResolution;
        }

        return isInteger(base, sourceText(), ParamOrdinal.SECOND);
    }

    @Override
    protected Pipe makePipe() {
        return new ToNumberFunctionPipe(source(), this, Expressions.pipe(value), Expressions.pipe(base));
    }

    @Override
    public boolean foldable() {
        return value.foldable() && base.foldable();
    }

    @Override
    public Object fold() {
        return doProcess(value.fold(), base.fold());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ToNumber::new, value, base);
    }

    @Override
    public ScriptTemplate asScript() {
        ScriptTemplate valueScript = asScript(value);
        ScriptTemplate baseScript = asScript(base);

        return new ScriptTemplate(format(Locale.ROOT, formatTemplate("{eql}.%s(%s,%s)"),
                "number",
                valueScript.template(),
                baseScript.template()),
                paramsBuilder()
                    .script(valueScript.params())
                    .script(baseScript.params())
                    .build(), dataType());
    }

    @Override
    public ScriptTemplate scriptWithField(FieldAttribute field) {
        return new ScriptTemplate(processScript(Scripts.DOC_VALUE),
                paramsBuilder().variable(field.exactAttribute().name()).build(),
                dataType());
    }

    @Override
    public DataType dataType() {
        return DataTypes.DOUBLE;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        if (newChildren.size() != 2) {
            throw new IllegalArgumentException("expected [2] children but received [" + newChildren.size() + "]");
        }

        return new ToNumber(source(), newChildren.get(0), newChildren.get(1));
    }
}
