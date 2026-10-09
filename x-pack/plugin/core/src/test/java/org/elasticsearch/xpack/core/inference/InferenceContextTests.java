/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.InferenceRequestMetadata;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Set;

import static org.elasticsearch.inference.InferenceRequestMetadata.Field.INTERACTION_ID;
import static org.elasticsearch.inference.InferenceRequestMetadata.Field.PRODUCT_FEATURE;
import static org.elasticsearch.inference.InferenceRequestMetadata.Field.PRODUCT_ORIGIN;
import static org.elasticsearch.inference.InferenceRequestMetadata.Field.PRODUCT_SOLUTION;
import static org.elasticsearch.inference.InferenceRequestMetadata.Field.PRODUCT_USE_CASE;
import static org.elasticsearch.inference.InferenceRequestMetadata.Field.SPACE_ID;
import static org.elasticsearch.inference.InferenceRequestMetadata.Field.TRACE_ID;
import static org.elasticsearch.inference.InferenceRequestMetadata.Field.USER_ID;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

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

    public void testXContentHasNoProductOrigin() throws IOException {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            createRandom().toXContent(builder, ToXContent.EMPTY_PARAMS);
            assertThat(Strings.toString(builder), not(containsString("product_origin")));
        }
    }

    /**
     * Pins the stream layout with literal writes, so reordering {@link InferenceContext#WIRE_LAYOUT} fails here
     * even though the reader and writer would still agree with each other.
     */
    private static BytesReference sevenStrings(
        String productUseCase,
        String productSolution,
        String productFeature,
        String interactionId,
        String traceId,
        String userId,
        String spaceId
    ) throws IOException {
        try (var out = new BytesStreamOutput()) {
            out.writeString(productUseCase);
            out.writeString(productSolution);
            out.writeString(productFeature);
            out.writeString(interactionId);
            out.writeString(traceId);
            out.writeString(userId);
            out.writeString(spaceId);
            return new BytesArray(BytesReference.toBytes(out.bytes()));
        }
    }

    private static BytesReference written(InferenceContext context) throws IOException {
        try (var out = new BytesStreamOutput()) {
            context.writeTo(out);
            return new BytesArray(BytesReference.toBytes(out.bytes()));
        }
    }

    public void testWriterProducesTheSevenStringLayout() throws IOException {
        var context = context("use-case", "solution", "feature", "interaction", "trace", "user", "space");

        assertThat(written(context), equalTo(sevenStrings("use-case", "solution", "feature", "interaction", "trace", "user", "space")));
    }

    public void testWriterWritesEmptyStringsForAbsentFields() throws IOException {
        var context = context("use-case", "", "", "interaction", "", "user", "");

        assertThat(written(context), equalTo(sevenStrings("use-case", "", "", "interaction", "", "user", "")));
        assertThat(written(InferenceContext.EMPTY_INSTANCE), equalTo(sevenStrings("", "", "", "", "", "", "")));
    }

    public void testReaderReadsTheSevenStringLayout() throws IOException {
        try (var in = sevenStrings("use-case", "", "feature", "", "trace", "", "space").streamInput()) {
            var context = new InferenceContext(in);

            assertThat(context.metadata().get(PRODUCT_USE_CASE), equalTo("use-case"));
            assertThat(context.metadata().get(PRODUCT_SOLUTION), nullValue());
            assertThat(context.metadata().get(PRODUCT_FEATURE), equalTo("feature"));
            assertThat(context.metadata().get(INTERACTION_ID), nullValue());
            assertThat(context.metadata().get(TRACE_ID), equalTo("trace"));
            assertThat(context.metadata().get(USER_ID), nullValue());
            assertThat(context.metadata().get(SPACE_ID), equalTo("space"));
            assertThat(in.available(), equalTo(0));
        }
    }

    public void testWireLayoutCoversExactlyTheInferencePropagatedFields() {
        assertThat(Set.copyOf(InferenceContext.WIRE_LAYOUT), equalTo(InferenceRequestMetadata.Field.INFERENCE_PROPAGATED));
        assertThat(InferenceContext.WIRE_LAYOUT.size(), equalTo(InferenceRequestMetadata.Field.INFERENCE_PROPAGATED.size()));
    }

    public void testConstructorRejectsProductOrigin() {
        var metadata = InferenceRequestMetadata.builder().put(PRODUCT_USE_CASE, "use-case").put(PRODUCT_ORIGIN, "kibana").build();

        var e = expectThrows(IllegalArgumentException.class, () -> new InferenceContext(metadata));
        assertThat(e.getMessage(), containsString("product_origin"));
    }

    public void testToStringIsTheMetadata() {
        var context = createRandom();
        assertThat(context.toString(), equalTo(context.metadata().toString()));
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
