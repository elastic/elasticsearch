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

/**
 * Stores profiling information about the query plan.  This can be the top level planning on the coordinating node, or the local
 * planning on the data nodes.
 */
public class PlannerProfile implements Writeable, ChunkedToXContentObject {

    public static final PlannerProfile EMPTY = new PlannerProfile(false, "");

    private final boolean isLocalPlanning;
    private final String nodeName;

    public static PlannerProfile readFrom(StreamInput in) throws IOException {
        boolean isLocalPlanning = in.readBoolean();
        String nodeName = in.readString();
        return new PlannerProfile(isLocalPlanning, nodeName);
    }

    public PlannerProfile(boolean isLocalPlanning, String nodeName) {
        this.isLocalPlanning = isLocalPlanning;
        this.nodeName = nodeName;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(isLocalPlanning);
        out.writeString(nodeName);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        // NOCOMMIT
        throw new UnsupportedOperationException();
    }

}
