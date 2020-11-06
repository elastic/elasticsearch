/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.string;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.BinaryPipe;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.Objects;

public class ConcatFunctionPipe extends BinaryPipe {

    public ConcatFunctionPipe(Source source, Expression expression, Pipe left, Pipe right) {
        super(source, expression, left, right);
    }

    @Override
    protected NodeInfo<ConcatFunctionPipe> info() {
        return NodeInfo.create(this, ConcatFunctionPipe::new, expression(), left(), right());
    }

    @Override
    protected BinaryPipe replaceChildren(Pipe left, Pipe right) {
        return new ConcatFunctionPipe(source(), expression(), left, right);
    }

    @Override
    public ConcatFunctionProcessor asProcessor() {
        return new ConcatFunctionProcessor(left().asProcessor(), right().asProcessor());
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(left(), right());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ConcatFunctionPipe other = (ConcatFunctionPipe) obj;
        return Objects.equals(left(), other.left())
                && Objects.equals(right(), other.right());
    }
}
