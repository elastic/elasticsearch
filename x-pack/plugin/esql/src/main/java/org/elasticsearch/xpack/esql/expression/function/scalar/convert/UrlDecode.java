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

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;

public final class UrlDecode extends UnaryScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "UrlDecode",
        UrlDecode::new
    );

    private UrlDecode(StreamInput in) throws IOException {
        super(in);
    }

    @FunctionInfo(
        returnType = "keyword",
        description = "URL-decodes the input, or returns `null` and adds a warning header to the response if the input cannot be decoded.",
        examples = { @Example(file = "string", tag = "url_decode") },
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA, version = "9.2.0") }
    )
    public UrlDecode(
        Source source,
        @Param(name = "string", type = { "keyword", "text" }, description = "The URL-encoded string to decode.") Expression str
    ) {
        super(source, str);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new UrlDecode(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, UrlDecode::new, field());
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
        return new UrlDecodeEvaluator.Factory(source(), toEvaluator.apply(field()));
    }

    @ConvertEvaluator(warnExceptions = { IllegalArgumentException.class })
    static BytesRef process(final BytesRef val) {
        String input = val.utf8ToString();
        String decoded = URLDecoder.decode(input, StandardCharsets.UTF_8);
        return new BytesRef(decoded);
    }
}
