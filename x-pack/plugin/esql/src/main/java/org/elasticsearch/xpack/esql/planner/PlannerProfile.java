/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Iterator;

public class PlannerProfile implements Writeable, ChunkedToXContentObject {

    public static final PlannerProfile EMPTY = new PlannerProfile();

    private final boolean isLocalPlanning;

    public PlannerProfile() {
        // NOCOMMIT
        throw new UnsupportedOperationException();
    }

    public PlannerProfile(StreamInput in) throws IOException {
        // NOCOMMIT
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // NOCOMMIT
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        // NOCOMMIT
        throw new UnsupportedOperationException();
    }

}
