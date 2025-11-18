/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;

public class BaseRequestManagerTests extends ESTestCase {
    public void testRateLimitGrouping_DifferentObjectReferences_HaveSameGroup() {
        int val1 = 1;
        int val2 = 1;

        var manager1 = new BaseRequestManager(mock(ThreadPool.class), "id", val1, new RateLimitSettings(1)) {
            @Override
            public void execute(
                InferenceInputs inferenceInputs,
                RequestSender requestSender,
                Supplier<Boolean> hasRequestCompletedFunction,
                ActionListener<InferenceServiceResults> listener
            ) {

            }
        };

        var manager2 = new BaseRequestManager(mock(ThreadPool.class), "id", val2, new RateLimitSettings(1)) {
            @Override
            public void execute(
                InferenceInputs inferenceInputs,
                RequestSender requestSender,
                Supplier<Boolean> hasRequestCompletedFunction,
                ActionListener<InferenceServiceResults> listener
            ) {

            }
        };

        assertThat(manager1.rateLimitGrouping(), is(manager2.rateLimitGrouping()));
    }

    public void testRateLimitGrouping_DifferentSettings_HaveDifferentGroup() {
        int val1 = 1;

        var manager1 = new BaseRequestManager(mock(ThreadPool.class), "id", val1, new RateLimitSettings(1)) {
            @Override
            public void execute(
                InferenceInputs inferenceInputs,
                RequestSender requestSender,
                Supplier<Boolean> hasRequestCompletedFunction,
                ActionListener<InferenceServiceResults> listener
            ) {

            }
        };

        var manager2 = new BaseRequestManager(mock(ThreadPool.class), "id", val1, new RateLimitSettings(2)) {
            @Override
            public void execute(
                InferenceInputs inferenceInputs,
                RequestSender requestSender,
                Supplier<Boolean> hasRequestCompletedFunction,
                ActionListener<InferenceServiceResults> listener
            ) {

            }
        };

        assertThat(manager1.rateLimitGrouping(), not(manager2.rateLimitGrouping()));
    }

    public void testRateLimitGrouping_DifferentSettingsTimeUnit_HaveDifferentGroup() {
        int val1 = 1;

        var manager1 = new BaseRequestManager(mock(ThreadPool.class), "id", val1, new RateLimitSettings(1, TimeUnit.MILLISECONDS)) {
            @Override
            public void execute(
                InferenceInputs inferenceInputs,
                RequestSender requestSender,
                Supplier<Boolean> hasRequestCompletedFunction,
                ActionListener<InferenceServiceResults> listener
            ) {

            }
        };

        var manager2 = new BaseRequestManager(mock(ThreadPool.class), "id", val1, new RateLimitSettings(1, TimeUnit.DAYS)) {
            @Override
            public void execute(
                InferenceInputs inferenceInputs,
                RequestSender requestSender,
                Supplier<Boolean> hasRequestCompletedFunction,
                ActionListener<InferenceServiceResults> listener
            ) {

            }
        };

        assertThat(manager1.rateLimitGrouping(), not(manager2.rateLimitGrouping()));
    }
}
