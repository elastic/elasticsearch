/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.io.IOException;
import java.util.List;

public class Sha256 extends AbstractHashFunction {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "SHA256", Sha256::new);

    private static final Hash.HashFunction SHA256 = Hash.HashFunction.create("SHA256");

    @FunctionInfo(
        returnType = "keyword",
        description = "Computes the SHA256 hash of the input.",
        examples = { @Example(file = "hash", tag = "sha256") }
    )
    public Sha256(Source source, @Param(name = "input", type = { "keyword", "text" }, description = "Input to hash.") Expression input) {
        super(source, input);
    }

    private Sha256(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected Hash.HashFunction getHashFunction() {
        return SHA256;
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Sha256(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Sha256::new, field);
    }
}
