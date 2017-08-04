/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin.sql.action;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

public class SqlResponse extends ActionResponse {

    private String sessionId;
    private long size;
    // NOCOMMIT: we probably need to add more info about columns, but that's all we use for now
    // NOCOMMIT: order of elements is important, so we might want to replace this with lists and
    // reflect this in generated JSON as well
    private Map<String, String> columns;
    private List<Map<String, Object>> rows;


    public SqlResponse() {
    }

    public SqlResponse(String sessionId, long size, Map<String, String> columns, List<Map<String, Object>> rows) {
        this.sessionId = sessionId;
        this.size = size;
        this.columns = columns;
        this.rows = rows;
    }

    public long size() {
        return size;
    }

    public Map<String, String> columns() {
        return columns;
    }

    public List<Map<String, Object>> rows() {
        return rows;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        sessionId = in.readOptionalString();
        size = in.readVLong();
        columns = in.readMap(StreamInput::readString, StreamInput::readString);
        rows = in.readList(StreamInput::readMap);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(sessionId);
        out.writeVLong(size);
        out.writeMap(columns, StreamOutput::writeString, StreamOutput::writeString);
        out.writeVInt(rows.size());
        for (Map<String, Object> row : rows) {
            out.writeMap(row);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SqlResponse that = (SqlResponse) o;
        return size == that.size &&
                Objects.equals(sessionId, that.sessionId) &&
                Objects.equals(columns, that.columns) &&
                Objects.equals(rows, that.rows);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sessionId, size, columns, rows);
    }
}
