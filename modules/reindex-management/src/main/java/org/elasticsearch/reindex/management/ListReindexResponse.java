/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v 3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex.management;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.reindex.management.GetReindexResponse.taskInfoToXContent;

public class ListReindexResponse extends BaseTasksResponse implements ChunkedToXContentObject {

    private final List<TaskInfo> tasks;

    public ListReindexResponse(
        List<TaskInfo> tasks,
        List<TaskOperationFailure> taskFailures,
        List<? extends ElasticsearchException> nodeFailures
    ) {
        super(taskFailures, nodeFailures);
        this.tasks = tasks == null ? List.of() : List.copyOf(tasks);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeCollection(tasks);
    }

    public List<TaskInfo> getTasks() {
        return tasks;
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return Iterators.concat(Iterators.single((builder, p) -> {
            builder.startObject();
            toXContentCommon(builder, p);
            builder.startArray("reindex");
            return builder;
        }), Iterators.map(tasks.iterator(), taskInfo -> (builder, p) -> {
            builder.startObject();
            taskInfoToXContent(builder, p, taskInfo);
            builder.endObject();
            return builder;
        }), Iterators.single((builder, p) -> {
            builder.endArray();
            builder.endObject();
            return builder;
        }));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ListReindexResponse that = (ListReindexResponse) o;
        return Objects.equals(tasks, that.tasks) && super.equals(that);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tasks, super.hashCode());
    }
}
