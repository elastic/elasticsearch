/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Physical plan node representing a lookup source operation.
 * This represents the source of a lookup query before conversion to operators.
 * The QueryList is created later during operator creation from a factory
 * and will receive the Page to operate on in the runtime
 * when we call queryList.getQuery(position, inputPage).
 */
public class ParameterizedQueryExec extends LeafExec {
    private final List<Attribute> output;

    public ParameterizedQueryExec(Source source, List<Attribute> output) {
        super(source);
        this.output = output;
    }

    @Override
    public List<Attribute> output() {
        return output;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    protected NodeInfo<ParameterizedQueryExec> info() {
        return NodeInfo.create(this, ParameterizedQueryExec::new, output);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ParameterizedQueryExec that = (ParameterizedQueryExec) o;
        return Objects.equals(output, that.output);
    }

    @Override
    public int hashCode() {
        return Objects.hash(output);
    }
}
