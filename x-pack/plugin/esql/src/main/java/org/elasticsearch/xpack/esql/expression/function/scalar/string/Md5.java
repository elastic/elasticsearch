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
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Hash.HashFunction;

import java.io.IOException;
import java.util.List;

public class Md5 extends AbstractHashFunction {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "MD5", Md5::new);

    private static final HashFunction MD5 = HashFunction.create("MD5");

    @FunctionInfo(
        returnType = "keyword",
        description = "Computes the MD5 hash of the input.",
        examples = { @Example(file = "hash", tag = "md5") }
    )
    public Md5(Source source, @Param(name = "input", type = { "keyword", "text" }, description = "Input to hash.") Expression input) {
        super(source, input);
    }

    private Md5(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected HashFunction getHashFunction() {
        return MD5;
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Md5(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Md5::new, field);
    }
}
