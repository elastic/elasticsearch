/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.monitor;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Class that represents the Health status for a node as determined by {@link NodeHealthService} and provides additional
 * info explaining the reasons
 */
public record StatusInfo(Status status, String info) implements Writeable {

    public StatusInfo(StreamInput in) throws IOException {
        this(readStatus(in), in.readOptionalString());
    }

    public enum Status {
        HEALTHY,
        UNHEALTHY
    }

    public String getInfo() {
        return info;
    }

    public Status getStatus() {
        return status;
    }

    private static Status readStatus(StreamInput in) throws IOException {
        String statusString = in.readOptionalString();
        if (statusString == null) {
            return null;
        }
        return Status.valueOf(statusString);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(status == null ? null : status.name());
        out.writeOptionalString(info);
    }

    @Override
    public String toString() {
        return "status[" + status + "]" + ", info[" + info + "]";
    }
}
