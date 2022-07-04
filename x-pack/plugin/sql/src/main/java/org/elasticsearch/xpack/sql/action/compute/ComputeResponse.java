/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.function.Supplier;

public class ComputeResponse extends ActionResponse {
    private final Supplier<Page> pageSupplier; // quick hack to stream responses back

    public ComputeResponse(StreamInput in) {
        throw new UnsupportedOperationException();
    }

    public ComputeResponse(Supplier<Page> pageSupplier) {
        super();
        this.pageSupplier = pageSupplier;
    }

    public Supplier<Page> getPageSupplier() {
        return pageSupplier;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }
}
