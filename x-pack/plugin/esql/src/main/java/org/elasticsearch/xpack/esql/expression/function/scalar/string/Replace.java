/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;

public class Replace extends EsqlScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Replace", Replace::new);

    private final Expression str;
    private final Expression regex;
    private final Expression newStr;

    @FunctionInfo(
        returnType = "keyword",
        description = """
            The function substitutes in the string `str` any match of the regular expression `regex`
            with the replacement string `newStr`.""",
        examples = @Example(
            file = "docs",
            tag = "replaceString",
            description = "This example replaces any occurrence of the word \"World\" with the word \"Universe\":"
        )
    )
    public Replace(
        Source source,
        @Param(name = "string", type = { "keyword", "text" }, description = "String expression.") Expression str,
        @Param(name = "regex", type = { "keyword", "text" }, description = "Regular expression.") Expression regex,
        @Param(name = "newString", type = { "keyword", "text" }, description = "Replacement string.") Expression newStr
    ) {
        super(source, Arrays.asList(str, regex, newStr));
        this.str = str;
        this.regex = regex;
        this.newStr = newStr;
    }

    private Replace(StreamInput in) throws IOException {
        this(
            Source.EMPTY,
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(str);
        out.writeNamedWriteable(regex);
        out.writeNamedWriteable(newStr);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return DataType.KEYWORD;
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

    @Evaluator(extraName = "Constant", warnExceptions = PatternSyntaxException.class)
    static BytesRef process(BytesRef str, @Fixed Pattern regex, BytesRef newStr) {
        if (str == null || regex == null || newStr == null) {
            return null;
        }
        return new BytesRef(regex.matcher(str.utf8ToString()).replaceAll(newStr.utf8ToString()));
    }

    @Evaluator(warnExceptions = PatternSyntaxException.class)
    static BytesRef process(BytesRef str, BytesRef regex, BytesRef newStr) {
        if (str == null) {
            return null;
        }

        if (regex == null || newStr == null) {
            return str;
        }
        return new BytesRef(str.utf8ToString().replaceAll(regex.utf8ToString(), newStr.utf8ToString()));
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
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        var strEval = toEvaluator.apply(str);
        var newStrEval = toEvaluator.apply(newStr);

        if (regex.foldable() && regex.dataType() == DataType.KEYWORD) {
            Pattern regexPattern;
            try {
                regexPattern = Pattern.compile(((BytesRef) regex.fold()).utf8ToString());
            } catch (PatternSyntaxException pse) {
                // TODO this is not right (inconsistent). See also https://github.com/elastic/elasticsearch/issues/100038
                // this should generate a header warning and return null (as do the rest of this functionality in evaluators),
                // but for the moment we let the exception through
                throw pse;
            }
            return new ReplaceConstantEvaluator.Factory(source(), strEval, regexPattern, newStrEval);
        }

        var regexEval = toEvaluator.apply(regex);
        return new ReplaceEvaluator.Factory(source(), strEval, regexEval, newStrEval);
    }

    Expression str() {
        return str;
    }

    Expression regex() {
        return regex;
    }

    Expression newStr() {
        return newStr;
    }
}
