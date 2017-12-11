/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.protocol.shared;

import java.io.DataInput;
import java.io.IOException;
import java.util.Objects;

/**
 * Superclass for responses both for {@link AbstractQueryInitRequest}
 * and {@link AbstractQueryPageRequest}.
 */
public abstract class AbstractQueryCloseResponse extends Response {
    private final boolean succeeded;

    protected AbstractQueryCloseResponse(boolean succeeded) {
        this.succeeded = succeeded;
    }

    protected AbstractQueryCloseResponse(Request request, DataInput in) throws IOException {
        succeeded = in.readBoolean();
    }

    @Override
    protected void writeTo(SqlDataOutput out) throws IOException {
        out.writeBoolean(succeeded);
    }

    /**
     * True if the cursor was really closed
     */
    public boolean succeeded() {
        return succeeded;
    }

    @Override
    protected String toStringBody() {
        return Boolean.toString(succeeded);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        AbstractQueryCloseResponse other = (AbstractQueryCloseResponse) obj;
        return succeeded == other.succeeded;
    }

    @Override
    public int hashCode() {
        return Objects.hash(succeeded);
    }
}
