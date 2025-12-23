/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.operator.lookup.LookupEnrichQueryGenerator;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Physical plan node representing a lookup source operation.
 * This represents the source of a lookup query before conversion to operators.
 * The QueryList is created during physical plan creation and will receive the Block at runtime.
 */
public class ParameterizedQueryExec extends LeafExec {
    private final List<Attribute> output;
    private final LookupEnrichQueryGenerator queryList;

    public ParameterizedQueryExec(Source source, List<Attribute> output, LookupEnrichQueryGenerator queryList) {
        super(source);
        this.output = output;
        this.queryList = queryList;
    }

    @Override
    public List<Attribute> output() {
        return output;
    }

    public LookupEnrichQueryGenerator queryList() {
        return queryList;
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
        return NodeInfo.create(this, ParameterizedQueryExec::new, output, queryList);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ParameterizedQueryExec that = (ParameterizedQueryExec) o;
        return Objects.equals(output, that.output) && Objects.equals(queryList, that.queryList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(output, queryList);
    }
}
