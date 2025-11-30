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
 * Physical plan for LOAD_RESULT command that fetches data from async query results.
 * This runs only on the coordinator node and is not serializable.
 */
public class LoadResultExec extends LeafExec {
    
    private final String searchId;
    private final List<Attribute> output;

    public LoadResultExec(Source source, String searchId, List<Attribute> output) {
        super(source);
        this.searchId = searchId;
        this.output = output;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("LoadResultExec is not serializable - it runs only on coordinator");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("LoadResultExec is not serializable - it runs only on coordinator");
    }

    public String searchId() {
        return searchId;
    }

    @Override
    public List<Attribute> output() {
        return output;
    }

    @Override
    protected NodeInfo<LoadResultExec> info() {
        return NodeInfo.create(this, LoadResultExec::new, searchId, output);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LoadResultExec that = (LoadResultExec) o;
        return Objects.equals(searchId, that.searchId) && Objects.equals(output, that.output);
    }

    @Override
    public int hashCode() {
        return Objects.hash(searchId, output);
    }
}

