/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.esql.expression.function.grouping.TsdimWithout;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;

/**
 * Query-dependent parameters that control how {@code _tsid} grouping is executed in TSDB.
 */
public record TsidGroupingParams(Set<String> excludedDimensions) implements Writeable {

    public static TsidGroupingParams from(TsdimWithout tsdimWithout) {
        return new TsidGroupingParams(tsdimWithout.excludedFieldNames());
    }

    public TsidGroupingParams(StreamInput in) throws IOException {
        this(in.readCollectionAsSet(StreamInput::readString));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringCollection(excludedDimensions);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TsidGroupingParams that = (TsidGroupingParams) o;
        return Objects.equals(excludedDimensions, that.excludedDimensions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(excludedDimensions);
    }
}
