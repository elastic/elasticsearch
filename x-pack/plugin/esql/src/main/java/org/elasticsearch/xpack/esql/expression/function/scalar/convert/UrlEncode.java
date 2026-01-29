/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.ann.ConvertEvaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.util.UrlCodecUtils;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.compute.ann.Fixed.Scope.THREAD_LOCAL;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;

public final class UrlEncode extends UnaryScalarFunction {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "UrlEncode",
        UrlEncode::new
    );

    private UrlEncode(StreamInput in) throws IOException {
        super(in);
    }

    @FunctionInfo(
        returnType = "keyword",
        description = "URL-encodes the input. All characters are {wikipedia}/Percent-encoding[percent-encoded] except "
            + "for alphanumerics, `.`, `-`, `_`, and `~`. Spaces are encoded as `+`.",
        examples = { @Example(file = "string", tag = "url_encode") },
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA, version = "9.2.0") }
    )
    public UrlEncode(
        Source source,
        @Param(name = "string", type = { "keyword", "text" }, description = "The URL to encode.") Expression str
    ) {
        super(source, str);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new UrlEncode(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, UrlEncode::new, field());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        return isString(field, sourceText(), TypeResolutions.ParamOrdinal.DEFAULT);
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return new UrlEncodeEvaluator.Factory(
            source(),
            toEvaluator.apply(field()),
            context -> new BreakingBytesRefBuilder(context.breaker(), "url_encode")
        );
    }

    @ConvertEvaluator()
    static BytesRef process(final BytesRef val, @Fixed(includeInToString = false, scope = THREAD_LOCAL) BreakingBytesRefBuilder scratch) {
        return UrlCodecUtils.urlEncode(val, scratch, true);
    }

}
