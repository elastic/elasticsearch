/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Profile results from a single {@link Driver}.
 */
public class DriverProfile implements Writeable, ChunkedToXContentObject {
    /**
     * Status of each {@link Operator} in the driver when it finishes.
     */
    private final List<DriverStatus.OperatorStatus> operators;

    public DriverProfile(List<DriverStatus.OperatorStatus> operators) {
        this.operators = operators;
    }

    public DriverProfile(StreamInput in) throws IOException {
        this.operators = in.readCollectionAsImmutableList(DriverStatus.OperatorStatus::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(operators);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return Iterators.concat(
            ChunkedToXContentHelper.startObject(),
            ChunkedToXContentHelper.array("operators", operators.iterator()),
            ChunkedToXContentHelper.endObject()
        );
    }
}
