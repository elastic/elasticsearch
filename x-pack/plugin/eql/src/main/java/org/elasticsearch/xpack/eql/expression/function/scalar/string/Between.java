/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.expression.function.scalar.string;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
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
import static org.elasticsearch.xpack.eql.expression.function.scalar.string.BetweenFunctionProcessor.doProcess;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isBoolean;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isStringAndExact;
import static org.elasticsearch.xpack.ql.expression.gen.script.ParamsBuilder.paramsBuilder;

/**
 * EQL specific between function.
 * between(source, left, right[, greedy=false, case_sensitive=false])
 * Extracts a substring from source that’s between left and right substrings
 */
public class Between extends ScalarFunction implements OptionalArgument {

    private final Expression input, left, right, greedy, caseSensitive;

    public Between(Source source, Expression input, Expression left, Expression right, Expression greedy, Expression caseSensitive) {
        super(source, Arrays.asList(input, left, right, toDefault(greedy), toDefault(caseSensitive)));
        this.input = input;
        this.left = left;
        this.right = right;
        this.greedy = arguments().get(3);
        this.caseSensitive = arguments().get(4);
    }

    private static Expression toDefault(Expression exp) {
        return exp != null ? exp : Literal.FALSE;
    }

    @Override
    protected TypeResolution resolveType() {
        if (!childrenResolved()) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = isStringAndExact(input, sourceText(), Expressions.ParamOrdinal.FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }

        resolution = isStringAndExact(left, sourceText(), Expressions.ParamOrdinal.SECOND);
        if (resolution.unresolved()) {
            return resolution;
        }

        resolution = isStringAndExact(right, sourceText(), Expressions.ParamOrdinal.THIRD);
        if (resolution.unresolved()) {
            return resolution;
        }

        resolution = isBoolean(greedy, sourceText(), Expressions.ParamOrdinal.FOURTH);
        if (resolution.unresolved()) {
            return resolution;
        }

        return isBoolean(caseSensitive, sourceText(), Expressions.ParamOrdinal.FIFTH);
    }

    @Override
    protected Pipe makePipe() {
        return new BetweenFunctionPipe(source(), this, Expressions.pipe(input),
                Expressions.pipe(left), Expressions.pipe(right),
                Expressions.pipe(greedy), Expressions.pipe(caseSensitive));
    }

    @Override
    public boolean foldable() {
        return input.foldable() && left.foldable() && right.foldable() && greedy.foldable() && caseSensitive.foldable();
    }

    @Override
    public Object fold() {
        return doProcess(input.fold(), left.fold(), right.fold(), greedy.fold(), caseSensitive.fold());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Between::new, input, left, right, greedy, caseSensitive);
    }

    @Override
    public ScriptTemplate asScript() {
        ScriptTemplate inputScript = asScript(input);
        ScriptTemplate leftScript = asScript(left);
        ScriptTemplate rightScript = asScript(right);
        ScriptTemplate greedyScript = asScript(greedy);
        ScriptTemplate caseSensitiveScript = asScript(caseSensitive);

        return asScriptFrom(inputScript, leftScript, rightScript, greedyScript, caseSensitiveScript);
    }

    protected ScriptTemplate asScriptFrom(ScriptTemplate inputScript, ScriptTemplate leftScript,
                                          ScriptTemplate rightScript, ScriptTemplate greedyScript, ScriptTemplate caseSensitiveScript) {
        return new ScriptTemplate(format(Locale.ROOT, formatTemplate("{eql}.%s(%s,%s,%s,%s,%s)"),
                "between",
                inputScript.template(),
                leftScript.template(),
                rightScript.template(),
                greedyScript.template(),
                caseSensitiveScript.template()),
                paramsBuilder()
                        .script(inputScript.params())
                        .script(leftScript.params())
                        .script(rightScript.params())
                        .script(greedyScript.params())
                        .script(caseSensitiveScript.params())
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
        return DataTypes.KEYWORD;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        if (newChildren.size() != 5) {
            throw new IllegalArgumentException("expected [5] children but received [" + newChildren.size() + "]");
        }

        return new Between(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2), newChildren.get(3), newChildren.get(4));
    }
}
