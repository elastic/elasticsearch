/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.inference.telemetry.InferenceProductContext.X_ELASTIC_INFERENCE_INTERACTION_ID_HTTP_HEADER;
import static org.elasticsearch.inference.telemetry.InferenceProductContext.X_ELASTIC_PRODUCT_FEATURE_HTTP_HEADER;
import static org.elasticsearch.inference.telemetry.InferenceProductContext.X_ELASTIC_PRODUCT_SOLUTION_HTTP_HEADER;
import static org.elasticsearch.inference.telemetry.InferenceProductContext.X_ELASTIC_PRODUCT_USE_CASE_HTTP_HEADER;
import static org.hamcrest.Matchers.equalTo;

public class InferenceContextTests extends AbstractWireSerializingTestCase<InferenceContext> {
    @Override
    protected Writeable.Reader<InferenceContext> instanceReader() {
        return InferenceContext::new;
    }

    @Override
    protected InferenceContext createTestInstance() {
        return createRandom();
    }

    public static InferenceContext createRandom() {
        return new InferenceContext(randomAlphaOfLength(10), randomAlphaOfLength(10), randomAlphaOfLength(10), randomAlphaOfLength(10));
    }

    @Override
    protected InferenceContext mutateInstance(InferenceContext instance) throws IOException {
        var components = new String[] {
            instance.productUseCase(),
            instance.productSolution(),
            instance.productFeature(),
            instance.interactionId() };
        var i = randomIntBetween(0, components.length - 1);
        components[i] = randomValueOtherThan(components[i], () -> randomAlphaOfLength(10));

        return new InferenceContext(components[0], components[1], components[2], components[3]);
    }

    public void testRejectsNullComponents() {
        expectThrows(NullPointerException.class, () -> new InferenceContext(null, "solution", "feature", "id"));
        expectThrows(NullPointerException.class, () -> new InferenceContext("use-case", null, "feature", "id"));
        expectThrows(NullPointerException.class, () -> new InferenceContext("use-case", "solution", null, "id"));
        expectThrows(NullPointerException.class, () -> new InferenceContext("use-case", "solution", "feature", null));
    }

    public void testFromHeaders() {
        var context = InferenceContext.fromHeaders(
            Map.of(
                X_ELASTIC_PRODUCT_USE_CASE_HTTP_HEADER,
                List.of("use-case"),
                X_ELASTIC_PRODUCT_SOLUTION_HTTP_HEADER,
                List.of("security"),
                X_ELASTIC_PRODUCT_FEATURE_HTTP_HEADER,
                List.of("attack_discovery"),
                X_ELASTIC_INFERENCE_INTERACTION_ID_HTTP_HEADER,
                List.of("interaction-id")
            )
        );

        assertThat(context, equalTo(new InferenceContext("use-case", "security", "attack_discovery", "interaction-id")));
    }

    public void testFromHeaders_UsesFirstValueOfEachHeader() {
        var headers = Map.of(X_ELASTIC_PRODUCT_USE_CASE_HTTP_HEADER, List.of("first", "second"));

        assertThat(InferenceContext.fromHeaders(headers).productUseCase(), equalTo("first"));
    }

    public void testFromHeaders_EmptyWhenHeadersAbsentOrValueless() {
        assertThat(InferenceContext.fromHeaders(null), equalTo(InferenceContext.EMPTY_INSTANCE));
        assertThat(InferenceContext.fromHeaders(Map.of()), equalTo(InferenceContext.EMPTY_INSTANCE));
        assertThat(
            InferenceContext.fromHeaders(Map.of(X_ELASTIC_PRODUCT_USE_CASE_HTTP_HEADER, List.of())),
            equalTo(InferenceContext.EMPTY_INSTANCE)
        );
    }

    public void testAttributionHeadersOmitsUnsetComponents() {
        assertThat(
            new InferenceContext("use-case", "", "", "interaction-id").attributionHeaders(),
            equalTo(
                Map.of(X_ELASTIC_PRODUCT_USE_CASE_HTTP_HEADER, "use-case", X_ELASTIC_INFERENCE_INTERACTION_ID_HTTP_HEADER, "interaction-id")
            )
        );
        assertThat(InferenceContext.EMPTY_INSTANCE.attributionHeaders(), equalTo(Map.of()));
    }
}
