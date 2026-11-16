/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;

import java.io.IOException;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;

/**
 * Page {@code 0} of a stored result: who ran the query, the shape of its rows and, once the query is done,
 * how it ended. Written before any page of rows and rewritten when the query finishes.
 *
 * @param headers   security headers of the user that ran the query, to check who may read the result
 * @param task      the task running the query, to tell whether a running result is still written
 * @param pageCount number of pages of rows, which follow this one from page {@code 1}
 * @param rowCount  number of rows across all pages
 */
public record StoredResultMetadata(
    Map<String, String> headers,
    TaskId task,
    List<ColumnInfoImpl> columns,
    ZoneId zoneId,
    Status status,
    int pageCount,
    long rowCount,
    long tookMillis,
    boolean partial,
    List<String> warnings,
    @Nullable Exception failure
) implements Writeable {

    public enum Status {
        RUNNING,
        COMPLETED,
        FAILED,
        CANCELLED
    }

    public static StoredResultMetadata running(Map<String, String> headers, TaskId task, List<ColumnInfoImpl> columns, ZoneId zoneId) {
        return new StoredResultMetadata(headers, task, columns, zoneId, Status.RUNNING, 0, 0, 0, false, List.of(), null);
    }

    public StoredResultMetadata(StreamInput in) throws IOException {
        this(
            in.readMap(StreamInput::readString),
            TaskId.readFromStream(in),
            in.readCollectionAsList(ColumnInfoImpl::new),
            in.readZoneId(),
            in.readEnum(Status.class),
            in.readVInt(),
            in.readVLong(),
            in.readVLong(),
            in.readBoolean(),
            in.readStringCollectionAsList(),
            in.readBoolean() ? in.readException() : null
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(headers, StreamOutput::writeString);
        task.writeTo(out);
        out.writeCollection(columns);
        out.writeZoneId(zoneId);
        out.writeEnum(status);
        out.writeVInt(pageCount);
        out.writeVLong(rowCount);
        out.writeVLong(tookMillis);
        out.writeBoolean(partial);
        out.writeStringCollection(warnings);
        out.writeBoolean(failure != null);
        if (failure != null) {
            out.writeException(failure);
        }
    }
}
