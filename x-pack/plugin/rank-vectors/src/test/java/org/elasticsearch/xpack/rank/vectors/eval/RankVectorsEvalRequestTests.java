/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.vectors.eval;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;

public class RankVectorsEvalRequestTests extends ESTestCase {

    public void testWireRoundTripAndCancellableChildren() throws Exception {
        RankVectorsEvalRequest request = new RankVectorsEvalRequest(
            new RankVectorsEvalSpec("vectors", new float[][] { { 1f, 0f } }, 1, 10, true),
            new String[] { "index-a", "index-b" }
        );
        request.indicesOptions(IndicesOptions.lenientExpandOpen());
        RankVectorsEvalRequest copy = copyWriteable(request, new NamedWriteableRegistry(List.of()), RankVectorsEvalRequest::new);
        assertEquals(request.spec(), copy.spec());
        assertArrayEquals(request.indices(), copy.indices());
        assertEquals(request.indicesOptions(), copy.indicesOptions());

        Task task = request.createTask(1, "transport", "indices:data/read/rank_vectors_eval", TaskId.EMPTY_TASK_ID, Map.of());
        assertTrue(task instanceof CancellableTask);
        assertTrue(((CancellableTask) task).shouldCancelChildrenOnCancellation());
    }
}
