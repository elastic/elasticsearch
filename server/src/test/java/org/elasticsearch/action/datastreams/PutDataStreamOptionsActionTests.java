/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.datastreams;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.cluster.metadata.DataStreamFailureStore;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.DataStreamOptions;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.nullValue;

public class PutDataStreamOptionsActionTests extends ESTestCase {

    public void testValidateRejectsFrozenAfterOnFailureStoreLifecycle() {
        DataStreamLifecycle lifecycle = DataStreamLifecycle.failuresLifecycleBuilder()
            .frozenAfter(new TimeValue(30, TimeUnit.DAYS))
            .build();
        DataStreamFailureStore failureStore = new DataStreamFailureStore(null, lifecycle);
        PutDataStreamOptionsAction.Request request = new PutDataStreamOptionsAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            new String[] { "my-data-stream" },
            failureStore
        );

        ActionRequestValidationException validationException = request.validate();

        assertNotNull(validationException);
        assertThat(
            validationException.getMessage(),
            containsString(DataStreamLifecycle.FROZEN_AFTER_NOT_SUPPORTED_ON_FAILURES_ERROR_MESSAGE)
        );
    }

    public void testValidateAcceptsFailureStoreLifecycleWithoutFrozenAfter() {
        DataStreamLifecycle lifecycle = DataStreamLifecycle.failuresLifecycleBuilder()
            .dataRetention(new TimeValue(30, TimeUnit.DAYS))
            .build();
        DataStreamFailureStore failureStore = new DataStreamFailureStore(null, lifecycle);
        PutDataStreamOptionsAction.Request request = new PutDataStreamOptionsAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            new String[] { "my-data-stream" },
            failureStore
        );

        ActionRequestValidationException validationException = request.validate();

        assertThat(validationException, nullValue());
    }

    public void testValidateAcceptsOptionsWithNoLifecycle() {
        DataStreamOptions options = DataStreamOptions.EMPTY;
        PutDataStreamOptionsAction.Request request = new PutDataStreamOptionsAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            new String[] { "my-data-stream" },
            options
        );

        ActionRequestValidationException validationException = request.validate();

        assertNotNull(validationException);
        assertThat(validationException.getMessage(), containsString("At least one option needs to be provided"));
    }
}
