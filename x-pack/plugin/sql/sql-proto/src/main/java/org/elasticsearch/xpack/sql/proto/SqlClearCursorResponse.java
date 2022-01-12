/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.proto;

import java.util.Objects;

/**
 * Response to the request to clean all SQL resources associated with the cursor for JDBC/CLI client
 */
public class SqlClearCursorResponse {

    private final boolean succeeded;

    public SqlClearCursorResponse(boolean succeeded) {
        this.succeeded = succeeded;
    }

    /**
     * @return Whether the attempt to clear a cursor was successful.
     */
    public boolean isSucceeded() {
        return succeeded;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SqlClearCursorResponse response = (SqlClearCursorResponse) o;
        return succeeded == response.succeeded;
    }

    @Override
    public int hashCode() {
        return Objects.hash(succeeded);
    }
}
