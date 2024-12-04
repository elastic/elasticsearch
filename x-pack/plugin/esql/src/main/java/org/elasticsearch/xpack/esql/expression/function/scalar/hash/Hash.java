/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.hash;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;

public class Hash extends EsqlScalarFunction {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Hash", Hash::new);

    private final Expression alg;
    private final Expression input;

    @FunctionInfo(
        returnType = "keyword",
        description = "Computes the hash of the input keyword."
    )
    public Hash(
        Source source,
        @Param(name = "alg", type = { "keyword", "text" }, description = "Hash algorithm to use.") Expression alg,
        @Param(name = "input", type = { "keyword", "text" }, description = "Input to hash.") Expression input
    ) {
        super(source, List.of(alg, input));
        this.alg = alg;
        this.input = input;
    }

    private Hash(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(alg);
        out.writeNamedWriteable(input);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return DataType.KEYWORD;
    }

    @Evaluator(warnExceptions = NoSuchAlgorithmException.class)
    static BytesRef process(BytesRef alg, BytesRef input) throws NoSuchAlgorithmException {
        byte[] digest = MessageDigest.getInstance(alg.utf8ToString()).digest(input.utf8ToString().getBytes());
        return new BytesRef(HexFormat.of().formatHex(digest));
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return new HashEvaluator.Factory(source(), toEvaluator.apply(alg), toEvaluator.apply(input));
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Hash(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Hash::new, children().get(0), children().get(1));
    }
}
