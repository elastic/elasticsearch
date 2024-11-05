/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.action;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class ReindexDataStreamPersistentTaskState implements Task.Status, PersistentTaskState {
    public static final String NAME = ReindexDataStreamTask.TASK_NAME;

    public ReindexDataStreamPersistentTaskState() {
        System.out.println("got here");
    }

    @Override
    public String getWriteableName() {
        return ReindexDataStreamTask.TASK_NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {}

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("some_state", "ooh this is state");
        builder.endObject();
        return builder;
    }

    public static PersistentTaskState fromXContent(XContentParser xContentParser) {
        return new ReindexDataStreamPersistentTaskState();
    }

    public static PersistentTaskState readFromStream(StreamInput streamInput) {
        return new ReindexDataStreamPersistentTaskState();
    }
}
