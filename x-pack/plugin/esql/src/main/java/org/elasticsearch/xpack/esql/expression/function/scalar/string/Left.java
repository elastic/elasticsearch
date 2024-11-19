/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;

/**
 * {code left(foo, len)} is an alias to {code substring(foo, 0, len)}
 */
public class Left extends EsqlScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Left", Left::new);

    private final Expression str;
    private final Expression length;

    @FunctionInfo(
        returnType = "keyword",
        description = "Returns the substring that extracts 'length' chars from 'string' starting from the left.",
        examples = { @Example(file = "string", tag = "left") }
    )
    public Left(
        Source source,
        @Param(name = "string", type = { "keyword", "text" }, description = "The string from which to return a substring.") Expression str,
        @Param(name = "length", type = { "integer" }, description = "The number of characters to return.") Expression length
    ) {
        super(source, Arrays.asList(str, length));
        this.str = str;
        this.length = length;
    }

    private Left(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(str);
        out.writeNamedWriteable(length);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Evaluator
    static BytesRef process(
        @Fixed(includeInToString = false, build = true) BytesRef out,
        @Fixed(includeInToString = false, build = true) UnicodeUtil.UTF8CodePoint cp,
        BytesRef str,
        int length
    ) {
        out.bytes = str.bytes;
        out.offset = str.offset;
        out.length = str.length;
        int curLenStart = 0;
        for (int i = 0; i < length && curLenStart < out.length; i++, curLenStart += cp.numBytes) {
            UnicodeUtil.codePointAt(out.bytes, out.offset + curLenStart, cp);
        }
        out.length = Math.min(curLenStart, out.length);
        return out;
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return new LeftEvaluator.Factory(
            source(),
            context -> new BytesRef(),
            context -> new UnicodeUtil.UTF8CodePoint(),
            toEvaluator.apply(str),
            toEvaluator.apply(length)
        );
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Left(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Left::new, str, length);
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

        resolution = TypeResolutions.isType(length, dt -> dt == INTEGER, sourceText(), SECOND, "integer");
        if (resolution.unresolved()) {
            return resolution;
        }

        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public boolean foldable() {
        return str.foldable() && length.foldable();
    }

    Expression str() {
        return str;
    }

    Expression length() {
        return length;
    }
}
