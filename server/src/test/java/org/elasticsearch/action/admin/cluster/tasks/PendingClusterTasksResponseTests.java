/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.tasks;

import org.elasticsearch.cluster.service.PendingClusterTask;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.Text;

import java.io.IOException;
import java.util.ArrayList;

public class PendingClusterTasksResponseTests extends ESTestCase {
    public void testPendingClusterTasksResponseChunking() {
        final var tasks = new ArrayList<PendingClusterTask>();
        for (int i = between(0, 10); i > 0; i--) {
            tasks.add(
                new PendingClusterTask(
                    randomNonNegativeLong(),
                    randomFrom(Priority.values()),
                    randomAlphaOfLengthBetween(1, 10),
                    randomNonNegativeLong(),
                    randomBoolean()
                )
            );
        }
        AbstractChunkedSerializingTestCase.assertChunkCount(
            new PendingClusterTasksResponse(tasks),
            response -> response.pendingTasks().size() + 2
        );
    }

    public void testPendingClusterTaskSerializationBwc() throws IOException {
        final var oldVersion = TransportVersionUtils.getPreviousVersion(PendingClusterTask.PENDING_CLUSTER_TASK_SOURCE_STRING);
        final var task = new PendingClusterTask(
            randomNonNegativeLong(),
            randomFrom(Priority.values()),
            randomAlphaOfLengthBetween(1, 10),
            randomNonNegativeLong(),
            randomBoolean()
        );

        try (var out = new BytesStreamOutput()) {
            out.setTransportVersion(oldVersion);
            task.writeTo(out);

            try (var in = out.bytes().streamInput()) {
                in.setTransportVersion(oldVersion);
                assertEquals(task.insertOrder(), in.readVLong());
                assertEquals(task.priority(), Priority.readFrom(in));
                assertEquals(task.source(), in.readText().string());
                assertEquals(task.timeInQueue(), in.readLong());
                assertEquals(task.executing(), in.readBoolean());
            }
        }

        try (var out = new BytesStreamOutput()) {
            out.setTransportVersion(oldVersion);
            out.writeVLong(task.insertOrder());
            Priority.writeTo(task.priority(), out);
            out.writeText(new Text(task.source()));
            out.writeLong(task.timeInQueue());
            out.writeBoolean(task.executing());

            try (var in = out.bytes().streamInput()) {
                in.setTransportVersion(oldVersion);
                assertEquals(task, new PendingClusterTask(in));
            }
        }
    }
}
