/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public class ShutdownPersistentTasksStatus implements Writeable, ToXContentObject {

    private final SingleNodeShutdownMetadata.Status status;

    public ShutdownPersistentTasksStatus() {
        this.status = SingleNodeShutdownMetadata.Status.COMPLETE;
    }

    public ShutdownPersistentTasksStatus(StreamInput in) throws IOException {
        this.status = SingleNodeShutdownMetadata.Status.COMPLETE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("status", status);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {

    }

    public SingleNodeShutdownMetadata.Status getStatus() {
        return status;
    }

    @Override
    public int hashCode() {
        return status.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ShutdownPersistentTasksStatus other = (ShutdownPersistentTasksStatus) obj;
        return status.equals(other.status);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
