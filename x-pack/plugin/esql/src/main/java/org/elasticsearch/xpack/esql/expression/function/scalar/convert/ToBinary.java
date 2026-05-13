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
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.io.IOException;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.type.DataType.BINARY;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;

/**
 * Converts a base64-encoded string into a {@code binary} value (raw bytes wrapped in a {@link BytesRef}).
 * Snapshot-only while the {@code binary} data type is under construction.
 */
public class ToBinary extends AbstractConvertFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "ToBinary",
        ToBinary::new
    );
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(ToBinary.class).unary(ToBinary::new).name("to_binary");

    private static final Map<DataType, BuildFactory> EVALUATORS = Map.ofEntries(
        Map.entry(BINARY, (source, fieldEval) -> fieldEval),
        Map.entry(KEYWORD, ToBinaryFromStringEvaluator.Factory::new),
        Map.entry(TEXT, ToBinaryFromStringEvaluator.Factory::new)
    );

    @FunctionInfo(
        returnType = "binary",
        preview = true,
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW) },
        description = "Converts a base64-encoded string into a `binary` value. "
            + "The input must be encoded with the standard (RFC 4648) base64 alphabet."
    )
    public ToBinary(
        Source source,
        @Param(
            name = "field",
            type = { "binary", "keyword", "text" },
            description = "Input value. The input can be a single- or multi-valued column or an expression."
        ) Expression field
    ) {
        super(source, field);
    }

    private ToBinary(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected Map<DataType, BuildFactory> factories() {
        return EVALUATORS;
    }

    @Override
    public DataType dataType() {
        return BINARY;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ToBinary(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ToBinary::new, field());
    }

    @ConvertEvaluator(extraName = "FromString", warnExceptions = { IllegalArgumentException.class })
    static BytesRef fromKeyword(BytesRef asString) {
        byte[] decoded = Base64.getDecoder().decode(asString.utf8ToString());
        return new BytesRef(decoded);
    }
}
