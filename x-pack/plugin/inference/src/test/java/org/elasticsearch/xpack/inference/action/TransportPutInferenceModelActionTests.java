/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.ServiceUtils;

import static org.hamcrest.Matchers.containsString;

public class TransportPutInferenceModelActionTests extends ESTestCase {

    public void testResolveTaskType() {

        assertEquals(TaskType.SPARSE_EMBEDDING, ServiceUtils.resolveTaskType(TaskType.SPARSE_EMBEDDING, null));
        assertEquals(TaskType.SPARSE_EMBEDDING, ServiceUtils.resolveTaskType(TaskType.ANY, TaskType.SPARSE_EMBEDDING.toString()));

        var e = expectThrows(ElasticsearchStatusException.class, () -> ServiceUtils.resolveTaskType(TaskType.ANY, null));
        assertThat(e.getMessage(), containsString("model is missing required setting [task_type]"));

        e = expectThrows(ElasticsearchStatusException.class, () -> ServiceUtils.resolveTaskType(TaskType.ANY, TaskType.ANY.toString()));
        assertThat(e.getMessage(), containsString("task_type [any] is not valid type for inference"));

        e = expectThrows(
            ElasticsearchStatusException.class,
            () -> ServiceUtils.resolveTaskType(TaskType.SPARSE_EMBEDDING, TaskType.TEXT_EMBEDDING.toString())
        );
        assertThat(
            e.getMessage(),
            containsString(
                "Cannot resolve conflicting task_type parameter in the request URL [sparse_embedding] and the request body [text_embedding]"
            )
        );
    }
}
