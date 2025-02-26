/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public class ShutdownPluginsStatus implements Writeable, ToXContentObject {

    private final SingleNodeShutdownMetadata.Status status;

    public ShutdownPluginsStatus(boolean safeToShutdown) {
        this.status = safeToShutdown ? SingleNodeShutdownMetadata.Status.COMPLETE : SingleNodeShutdownMetadata.Status.IN_PROGRESS;
    }

    public ShutdownPluginsStatus(StreamInput in) throws IOException {
        this.status = in.readEnum(SingleNodeShutdownMetadata.Status.class);
    }

    public SingleNodeShutdownMetadata.Status getStatus() {
        return this.status;
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
        out.writeEnum(this.status);
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
        ShutdownPluginsStatus other = (ShutdownPluginsStatus) obj;
        return status.equals(other.status);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
