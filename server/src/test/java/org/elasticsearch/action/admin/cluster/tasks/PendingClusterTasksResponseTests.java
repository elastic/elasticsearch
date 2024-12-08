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
import org.elasticsearch.common.text.Text;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;

public class PendingClusterTasksResponseTests extends ESTestCase {
    public void testPendingClusterTasksResponseChunking() {
        final var tasks = new ArrayList<PendingClusterTask>();
        for (int i = between(0, 10); i > 0; i--) {
            tasks.add(
                new PendingClusterTask(
                    randomNonNegativeLong(),
                    randomFrom(Priority.values()),
                    new Text(randomAlphaOfLengthBetween(1, 10)),
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
}
