/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.readiness;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

public final class ReadinessResponse extends ActionResponse {
    final boolean isReady;

    public ReadinessResponse(boolean isReady) {
        this.isReady = isReady;
    }

    public boolean isReady() {
        return isReady;
    }

    public static ReadinessResponse readFrom(StreamInput in) throws IOException {
        return new ReadinessResponse(in.readBoolean());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(isReady);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ReadinessResponse that = (ReadinessResponse) o;
        return isReady == that.isReady;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(isReady);
    }

    @Override
    public String toString() {
        return "ReadinessResponse{" + "isReady=" + isReady + '}';
    }

}
