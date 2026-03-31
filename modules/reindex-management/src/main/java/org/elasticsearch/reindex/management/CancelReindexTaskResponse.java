/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex.management;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.tasks.TaskResult;

import java.io.IOException;
import java.util.Optional;

/** Response to a single reindex task cancel action. */
public class CancelReindexTaskResponse implements Writeable {

    @Nullable
    private final TaskResult completedTaskResult;

    public CancelReindexTaskResponse(@Nullable final TaskResult completedTaskResult) {
        this.completedTaskResult = completedTaskResult;
    }

    public CancelReindexTaskResponse(final StreamInput in) throws IOException {
        this.completedTaskResult = in.readOptionalWriteable(TaskResult::new);
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeOptionalWriteable(completedTaskResult);
    }

    public Optional<TaskResult> getCompletedTaskResult() {
        return Optional.ofNullable(completedTaskResult);
    }
}
