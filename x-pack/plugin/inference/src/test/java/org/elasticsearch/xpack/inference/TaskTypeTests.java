/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import static org.hamcrest.core.Is.is;

public class TaskTypeTests extends ESTestCase {

    public void testFromStringOrStatusException() {
        var exception = expectThrows(ElasticsearchStatusException.class, () -> TaskType.fromStringOrStatusException(null));
        assertThat(exception.getMessage(), Matchers.is("Task type must not be null"));

        exception = expectThrows(ElasticsearchStatusException.class, () -> TaskType.fromStringOrStatusException("blah"));
        assertThat(exception.getMessage(), Matchers.is("Unknown task_type [blah]"));

        assertThat(TaskType.fromStringOrStatusException("any"), Matchers.is(TaskType.ANY));
    }

    public void testIsCompatibleWithSemanticText() {
        assertThat(TaskType.ANY.isCompatibleWithSemanticText(), is(false));
        assertThat(TaskType.CHAT_COMPLETION.isCompatibleWithSemanticText(), is(false));
        assertThat(TaskType.COMPLETION.isCompatibleWithSemanticText(), is(false));
        assertThat(TaskType.RERANK.isCompatibleWithSemanticText(), is(false));
        assertThat(TaskType.TEXT_EMBEDDING.isCompatibleWithSemanticText(), is(true));
        assertThat(TaskType.SPARSE_EMBEDDING.isCompatibleWithSemanticText(), is(true));
    }
}
