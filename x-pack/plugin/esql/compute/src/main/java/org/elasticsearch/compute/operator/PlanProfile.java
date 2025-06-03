/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public record PlanProfile(String description, String clusterName, String nodeName, String planTree) implements Writeable, ToXContentObject {

    public static PlanProfile readFrom(StreamInput in) throws IOException {
        return new PlanProfile(in.readString(), in.readString(), in.readString(), in.readString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(description);
        out.writeString(clusterName);
        out.writeString(nodeName);
        out.writeString(planTree);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject()
            .field("description", description)
            .field("cluster_name", clusterName)
            .field("node_name", nodeName)
            .field("plan", planTree)
            .endObject();
    }
}
