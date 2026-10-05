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

import static org.elasticsearch.inference.InferenceRequestMetadata.Field.INTERACTION_ID;
import static org.elasticsearch.inference.InferenceRequestMetadata.Field.PRODUCT_FEATURE;
import static org.elasticsearch.inference.InferenceRequestMetadata.Field.PRODUCT_SOLUTION;
import static org.elasticsearch.inference.InferenceRequestMetadata.Field.PRODUCT_USE_CASE;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class InferenceProductContextTests extends ESTestCase {

    public void testCreate_ReadsUseCaseAndOriginOnly() {
        var threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putHeader(PRODUCT_USE_CASE.httpHeader(), "security ai assistant");
        threadContext.putHeader(Task.X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER, "kibana");
        threadContext.putHeader(PRODUCT_SOLUTION.httpHeader(), "security");
        threadContext.putHeader(PRODUCT_FEATURE.httpHeader(), "attack_discovery");
        threadContext.putHeader(INTERACTION_ID.httpHeader(), "interaction-id");

        assertThat(InferenceProductContext.create(threadContext), is(new InferenceProductContext("security ai assistant", "kibana")));
    }

    public void testCreate_ReturnsEmptyInstanceWhenHeadersAreAbsent() {
        var context = InferenceProductContext.create(new ThreadContext(Settings.EMPTY));

        assertThat(context, sameInstance(InferenceProductContext.EMPTY));
    }

    public void testCreate_IgnoresRequestMetadataHeaders() {
        for (var header : new String[] { PRODUCT_SOLUTION.httpHeader(), PRODUCT_FEATURE.httpHeader(), INTERACTION_ID.httpHeader() }) {
            var threadContext = new ThreadContext(Settings.EMPTY);
            threadContext.putHeader(header, "present");

            assertThat(InferenceProductContext.create(threadContext), sameInstance(InferenceProductContext.EMPTY));
        }
    }

    public static InferenceProductContext randomInferenceProductContext() {
        return new InferenceProductContext(randomFrom(randomAlphaOfLength(10), null), randomFrom(randomAlphaOfLength(10), null));
    }
}
