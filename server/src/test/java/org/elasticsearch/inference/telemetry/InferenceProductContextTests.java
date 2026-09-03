/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference.telemetry;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.elasticsearch.inference.telemetry.InferenceProductContext.X_ELASTIC_INFERENCE_INTERACTION_ID_HTTP_HEADER;
import static org.elasticsearch.inference.telemetry.InferenceProductContext.X_ELASTIC_PRODUCT_FEATURE_HTTP_HEADER;
import static org.elasticsearch.inference.telemetry.InferenceProductContext.X_ELASTIC_PRODUCT_SOLUTION_HTTP_HEADER;
import static org.elasticsearch.inference.telemetry.InferenceProductContext.X_ELASTIC_PRODUCT_USE_CASE_HTTP_HEADER;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class InferenceProductContextTests extends ESTestCase {

    public void testCreate_ReadsHeadersFromThreadContext() {
        var expectedContext = randomInferenceProductContext();

        var threadContext = new ThreadContext(Settings.EMPTY);
        if (expectedContext.productUseCase() != null) {
            threadContext.putHeader(X_ELASTIC_PRODUCT_USE_CASE_HTTP_HEADER, expectedContext.productUseCase());
        }

        if (expectedContext.productOrigin() != null) {
            threadContext.putHeader(Task.X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER, expectedContext.productOrigin());
        }

        if (expectedContext.productSolution() != null) {
            threadContext.putHeader(X_ELASTIC_PRODUCT_SOLUTION_HTTP_HEADER, expectedContext.productSolution());
        }

        if (expectedContext.productFeature() != null) {
            threadContext.putHeader(X_ELASTIC_PRODUCT_FEATURE_HTTP_HEADER, expectedContext.productFeature());
        }

        if (expectedContext.interactionId() != null) {
            threadContext.putHeader(X_ELASTIC_INFERENCE_INTERACTION_ID_HTTP_HEADER, expectedContext.interactionId());
        }

        var createdContext = InferenceProductContext.create(threadContext);

        assertThat(createdContext, is(expectedContext));
    }

    public void testCreate_ReturnsEmptyInstanceWhenHeadersAreAbsent() {
        var context = InferenceProductContext.create(new ThreadContext(Settings.EMPTY));

        assertThat(context, sameInstance(InferenceProductContext.EMPTY));
    }

    public void testCreate_DoesNotReturnEmptyWhenOnlyOneOptionalHeaderIsPresent() {
        record Case(String header, String value, InferenceProductContext expected) {}

        for (var testCase : List.of(
            new Case(
                X_ELASTIC_PRODUCT_SOLUTION_HTTP_HEADER,
                "security",
                new InferenceProductContext(null, null, "security", null, null)
            ),
            new Case(
                X_ELASTIC_PRODUCT_FEATURE_HTTP_HEADER,
                "attack_discovery",
                new InferenceProductContext(null, null, null, "attack_discovery", null)
            ),
            new Case(
                X_ELASTIC_INFERENCE_INTERACTION_ID_HTTP_HEADER,
                "interaction-id",
                new InferenceProductContext(null, null, null, null, "interaction-id")
            )
        )) {
            var threadContext = new ThreadContext(Settings.EMPTY);
            threadContext.putHeader(testCase.header(), testCase.value());

            assertThat(InferenceProductContext.create(threadContext), is(testCase.expected()));
        }
    }

    public static InferenceProductContext randomInferenceProductContext() {
        return new InferenceProductContext(
            randomFrom(randomAlphaOfLength(10), null),
            randomFrom(randomAlphaOfLength(10), null),
            randomFrom(randomAlphaOfLength(10), null),
            randomFrom(randomAlphaOfLength(10), null),
            randomFrom(randomAlphaOfLength(10), null)
        );
    }
}
