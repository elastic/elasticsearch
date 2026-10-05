/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.InferenceRequestMetadata;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;

import static org.elasticsearch.inference.InferenceRequestMetadata.Field.INTERACTION_ID;
import static org.elasticsearch.inference.InferenceRequestMetadata.Field.PRODUCT_FEATURE;
import static org.elasticsearch.inference.InferenceRequestMetadata.Field.PRODUCT_SOLUTION;
import static org.elasticsearch.inference.InferenceRequestMetadata.Field.PRODUCT_USE_CASE;
import static org.elasticsearch.inference.InferenceRequestMetadata.Field.SPACE_ID;
import static org.elasticsearch.inference.InferenceRequestMetadata.Field.TRACE_ID;
import static org.elasticsearch.inference.InferenceRequestMetadata.Field.USER_ID;
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
        return context(
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10)
        );
    }

    @Override
    protected InferenceContext mutateInstance(InferenceContext instance) {
        var components = new String[] {
            valueOrEmpty(instance, PRODUCT_USE_CASE),
            valueOrEmpty(instance, PRODUCT_SOLUTION),
            valueOrEmpty(instance, PRODUCT_FEATURE),
            valueOrEmpty(instance, INTERACTION_ID),
            valueOrEmpty(instance, TRACE_ID),
            valueOrEmpty(instance, USER_ID),
            valueOrEmpty(instance, SPACE_ID) };
        var i = randomIntBetween(0, components.length - 1);
        components[i] = randomValueOtherThan(components[i], () -> randomAlphaOfLength(10));
        return context(components[0], components[1], components[2], components[3], components[4], components[5], components[6]);
    }

    public void testOneArgConstructorKeepsOnlyUseCase() {
        var context = new InferenceContext("esql");
        assertThat(context.metadata().get(PRODUCT_USE_CASE), equalTo("esql"));
        assertThat(context.metadata().get(PRODUCT_SOLUTION), equalTo(null));
        assertThat(context.metadata().get(PRODUCT_FEATURE), equalTo(null));
        assertThat(context.metadata().get(INTERACTION_ID), equalTo(null));
        assertThat(context.metadata().get(TRACE_ID), equalTo(null));
        assertThat(context.metadata().get(USER_ID), equalTo(null));
        assertThat(context.metadata().get(SPACE_ID), equalTo(null));
    }

    public void testOneArgConstructorRejectsNull() {
        expectThrows(NullPointerException.class, () -> new InferenceContext((String) null));
    }

    public void testXContentWritesEmptyStringForAbsentFields() throws IOException {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            new InferenceContext("esql").toXContent(builder, ToXContent.EMPTY_PARAMS);
            assertThat(
                Strings.toString(builder),
                equalTo(
                    "{\"product_use_case\":\"esql\",\"product_solution\":\"\",\"product_feature\":\"\",\"interaction_id\":\"\","
                        + "\"trace_id\":\"\",\"user_id\":\"\",\"space_id\":\"\"}"
                )
            );
        }
    }

    private static String valueOrEmpty(InferenceContext instance, InferenceRequestMetadata.Field field) {
        var value = instance.metadata().get(field);
        return value == null ? "" : value;
    }

    public static InferenceContext context(String productUseCase, String productSolution, String productFeature, String interactionId) {
        return context(productUseCase, productSolution, productFeature, interactionId, "", "", "");
    }

    public static InferenceContext context(
        String productUseCase,
        String productSolution,
        String productFeature,
        String interactionId,
        String traceId,
        String userId,
        String spaceId
    ) {
        return new InferenceContext(
            InferenceRequestMetadata.builder()
                .put(PRODUCT_USE_CASE, productUseCase)
                .put(PRODUCT_SOLUTION, productSolution)
                .put(PRODUCT_FEATURE, productFeature)
                .put(INTERACTION_ID, interactionId)
                .put(TRACE_ID, traceId)
                .put(USER_ID, userId)
                .put(SPACE_ID, spaceId)
                .build()
        );
    }
}
