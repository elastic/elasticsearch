/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.utils;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.indices.IndexCreationException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class ExceptionsHelperTests extends ESTestCase {

    public void testFindSearchExceptionRootCause_GivenWrappedSearchPhaseException() {
        SearchPhaseExecutionException searchPhaseExecutionException = new SearchPhaseExecutionException(
            "test-phase",
            "partial shards failure",
            new ShardSearchFailure[] { new ShardSearchFailure(new ElasticsearchException("for the cause!")) }
        );

        Throwable rootCauseException = ExceptionsHelper.findSearchExceptionRootCause(
            new IndexCreationException("test-index", searchPhaseExecutionException)
        );

        assertThat(rootCauseException.getMessage(), equalTo("for the cause!"));
    }

    public void testFindSearchExceptionRootCause_GivenRuntimeException() {
        RuntimeException runtimeException = new RuntimeException("nothing to unwrap here");
        assertThat(ExceptionsHelper.findSearchExceptionRootCause(runtimeException), sameInstance(runtimeException));
    }

    public void testFindSearchExceptionRootCause_GivenWrapperException() {
        RuntimeException runtimeException = new RuntimeException("cause");

        Throwable rootCauseException = ExceptionsHelper.findSearchExceptionRootCause(
            new IndexCreationException("test-index", runtimeException)
        );

        assertThat(rootCauseException.getMessage(), equalTo("cause"));
    }

    public void testTaskOperationFailureToStatusException() {
        var rootCause = new IllegalArgumentException("bah");
        var failure = new TaskOperationFailure("foo", 0L, rootCause);
        var convertedException = ExceptionsHelper.taskOperationFailureToStatusException(failure);
        assertThat(convertedException.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(convertedException.getCause(), sameInstance(rootCause));
    }
}
