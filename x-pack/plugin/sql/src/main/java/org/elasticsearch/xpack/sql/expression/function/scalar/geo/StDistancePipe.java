/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.geo;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.BinaryPipe;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.tree.Source;

import java.util.Objects;

public class StDistancePipe extends BinaryPipe {

    public StDistancePipe(Source source, Expression expression, Pipe left, Pipe right) {
        super(source, expression, left, right);
    }

    @Override
    protected NodeInfo<StDistancePipe> info() {
        return NodeInfo.create(this, StDistancePipe::new, expression(), left(), right());
    }

    @Override
    protected BinaryPipe replaceChildren(Pipe left, Pipe right) {
        return new StDistancePipe(source(), expression(), left, right);
    }

    @Override
    public StDistanceProcessor asProcessor() {
        return new StDistanceProcessor(left().asProcessor(), right().asProcessor());
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

        StDistancePipe other = (StDistancePipe) obj;
        return Objects.equals(left(), other.left())
                && Objects.equals(right(), other.right());
    }
}
