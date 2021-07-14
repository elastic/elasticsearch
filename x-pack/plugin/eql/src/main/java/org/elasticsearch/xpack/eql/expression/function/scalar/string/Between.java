/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.expression.function.scalar.string;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.ql.expression.function.scalar.string.CaseInsensitiveScalarFunction;
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
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.FOURTH;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isBoolean;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isStringAndExact;
import static org.elasticsearch.xpack.ql.expression.gen.script.ParamsBuilder.paramsBuilder;

/**
 * EQL specific between function.
 * between(source, left, right[, greedy=false])
 * Extracts a substring from source that’s between left and right substrings
 */
public class Between extends CaseInsensitiveScalarFunction implements OptionalArgument {

    private final Expression input, left, right, greedy;

    public Between(Source source, Expression input, Expression left, Expression right, Expression greedy, boolean caseInsensitive) {
        super(source, Arrays.asList(input, left, right, defaultGreedy(greedy)), caseInsensitive);
        this.input = input;
        this.left = left;
        this.right = right;
        this.greedy = arguments().get(3);
    }

    private static Expression defaultGreedy(Expression exp) {
        return exp != null ? exp : Literal.FALSE;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = isStringAndExact(input, sourceText(), FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }

        resolution = isStringAndExact(left, sourceText(), SECOND);
        if (resolution.unresolved()) {
            return resolution;
        }

        resolution = isStringAndExact(right, sourceText(), THIRD);
        if (resolution.unresolved()) {
            return resolution;
        }

        return isBoolean(greedy, sourceText(), FOURTH);
    }

    public Expression input() {
        return input;
    }

    public Expression left() {
        return left;
    }

    public Expression right() {
        return right;
    }

    public Expression greedy() {
        return greedy;
    }

    @Override
    protected Pipe makePipe() {
        return new BetweenFunctionPipe(source(), this, Expressions.pipe(input),
            Expressions.pipe(left), Expressions.pipe(right),
            Expressions.pipe(greedy), isCaseInsensitive());
    }

    @Override
    public boolean foldable() {
        return input.foldable() && left.foldable() && right.foldable() && greedy.foldable();
    }

    @Override
    public Object fold() {
        return doProcess(input.fold(), left.fold(), right.fold(), greedy.fold(), isCaseInsensitive());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Between::new, input, left, right, greedy, isCaseInsensitive());
    }

    @Override
    public ScriptTemplate asScript() {
        ScriptTemplate inputScript = asScript(input);
        ScriptTemplate leftScript = asScript(left);
        ScriptTemplate rightScript = asScript(right);
        ScriptTemplate greedyScript = asScript(greedy);

        return asScriptFrom(inputScript, leftScript, rightScript, greedyScript);
    }

    protected ScriptTemplate asScriptFrom(ScriptTemplate inputScript, ScriptTemplate leftScript,
                                          ScriptTemplate rightScript, ScriptTemplate greedyScript) {
        return new ScriptTemplate(format(Locale.ROOT, formatTemplate("{eql}.%s(%s,%s,%s,%s,%s)"),
            "between",
            inputScript.template(),
            leftScript.template(),
            rightScript.template(),
            greedyScript.template(),
            "{}"),
            paramsBuilder()
                .script(inputScript.params())
                .script(leftScript.params())
                .script(rightScript.params())
                .script(greedyScript.params())
                .variable(isCaseInsensitive())
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
        return new Between(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2), newChildren.get(3), isCaseInsensitive());
    }
}
