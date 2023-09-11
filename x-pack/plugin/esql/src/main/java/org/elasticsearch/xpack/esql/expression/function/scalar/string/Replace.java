/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isString;

public class Replace extends ScalarFunction implements EvaluatorMapper {

    private final Expression str;
    private final Expression newStr;
    private final Expression regex;

    public Replace(Source source, Expression str, Expression regex, Expression newStr) {
        super(source, Arrays.asList(str, regex, newStr));
        this.str = str;
        this.regex = regex;
        this.newStr = newStr;
    }

    @Override
    public DataType dataType() {
        return DataTypes.KEYWORD;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = isString(str, sourceText(), FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }

        resolution = isString(regex, sourceText(), SECOND);
        if (resolution.unresolved()) {
            return resolution;
        }

        return isString(newStr, sourceText(), THIRD);
    }

    @Override
    public boolean foldable() {
        return str.foldable() && regex.foldable() && newStr.foldable();
    }

    @Override
    public Object fold() {
        return EvaluatorMapper.super.fold();
    }

    @Evaluator(extraName = "Constant")
    static BytesRef process(BytesRef str, @Fixed Pattern regex, BytesRef newStr) {
        if (str == null || regex == null || newStr == null) {
            return null;
        }

        try {
            return new BytesRef(regex.matcher(str.utf8ToString()).replaceAll(newStr.utf8ToString()));
        } catch (PatternSyntaxException ex) {
            // let's return the original string as if there is no match
            // we don't want to throw a runtime error
            return str;
        }
    }

    @Evaluator(extraName = "Passthrough")
    static BytesRef process(BytesRef str) {
        return str;
    }

    @Evaluator
    static BytesRef process(BytesRef str, BytesRef regex, BytesRef newStr) {
        if (str == null) {
            return null;
        }

        if (regex == null || newStr == null) {
            return str;
        }

        try {
            return new BytesRef(str.utf8ToString().replaceAll(regex.utf8ToString(), newStr.utf8ToString()));
        } catch (PatternSyntaxException ex) {
            // let's return the original string as if there is no match
            // we don't want to throw a runtime error
            return str;
        }
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Replace(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Replace::new, str, regex, newStr);
    }

    @Override
    public ScriptTemplate asScript() {
        throw new UnsupportedOperationException("functions do not support scripting");
    }

    @Override
    public Supplier<EvalOperator.ExpressionEvaluator> toEvaluator(
        Function<Expression, Supplier<EvalOperator.ExpressionEvaluator>> toEvaluator
    ) {
        Supplier<EvalOperator.ExpressionEvaluator> strEval = toEvaluator.apply(str);
        Supplier<EvalOperator.ExpressionEvaluator> newStrEval = toEvaluator.apply(newStr);

        if (regex.foldable() && regex.dataType() == DataTypes.KEYWORD) {
            try {
                Pattern regexPattern = Pattern.compile(((BytesRef) regex.fold()).utf8ToString());
                return () -> new ReplaceConstantEvaluator(strEval.get(), regexPattern, newStrEval.get());
            } catch (PatternSyntaxException ex) {
                // let's return the original string as if there is no match
                return () -> new ReplacePassthroughEvaluator(strEval.get());
            }
        }

        Supplier<EvalOperator.ExpressionEvaluator> regexEval = toEvaluator.apply(regex);
        return () -> new ReplaceEvaluator(strEval.get(), regexEval.get(), newStrEval.get());
    }
}
