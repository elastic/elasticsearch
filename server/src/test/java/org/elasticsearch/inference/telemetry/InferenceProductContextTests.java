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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;

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

    public void testCreate_DoesNotReturnEmptyWhenOnlySolutionIsPresent() {
        var threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putHeader(X_ELASTIC_PRODUCT_SOLUTION_HTTP_HEADER, "security");

        var context = InferenceProductContext.create(threadContext);

        assertThat(context, is(new InferenceProductContext(null, null, "security", null, null)));
    }

    public void testCreate_DoesNotReturnEmptyWhenOnlyFeatureIsPresent() {
        var threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putHeader(X_ELASTIC_PRODUCT_FEATURE_HTTP_HEADER, "attack_discovery");

        var context = InferenceProductContext.create(threadContext);

        assertThat(context, is(new InferenceProductContext(null, null, null, "attack_discovery", null)));
    }

    public void testCreate_DoesNotReturnEmptyWhenOnlyInteractionIdIsPresent() {
        var threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putHeader(X_ELASTIC_INFERENCE_INTERACTION_ID_HTTP_HEADER, "interaction-id");

        var context = InferenceProductContext.create(threadContext);

        assertThat(context, is(new InferenceProductContext(null, null, null, null, "interaction-id")));
    }

    /**
     * An {@link InferenceProductContext} carrying only a use case and an origin, for tests that predate solution, feature and
     * interaction id and do not care about them.
     */
    public static InferenceProductContext productContext(@Nullable String productUseCase, @Nullable String productOrigin) {
        return new InferenceProductContext(productUseCase, productOrigin, null, null, null);
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
