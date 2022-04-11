/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.proto;

import java.util.Objects;

/**
 * Request to clean all SQL resources associated with the cursor for JDBC/CLI client
 */
public class SqlClearCursorRequest extends AbstractSqlRequest {

    private final String cursor;
    private final Boolean binaryCommunication;

    public SqlClearCursorRequest(String cursor, RequestInfo requestInfo, Boolean binaryCommunication) {
        super(requestInfo);
        this.cursor = cursor;
        this.binaryCommunication = binaryCommunication;
    }

    public String getCursor() {
        return cursor;
    }

    public Boolean binaryCommunication() {
        return binaryCommunication;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        SqlClearCursorRequest that = (SqlClearCursorRequest) o;
        return Objects.equals(cursor, that.cursor) && Objects.equals(binaryCommunication, that.binaryCommunication);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), cursor, binaryCommunication);
    }
}
