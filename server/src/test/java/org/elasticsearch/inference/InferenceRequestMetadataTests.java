/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.test.ESTestCase;

import java.util.EnumMap;
import java.util.Map;

import static org.elasticsearch.inference.InferenceRequestMetadata.Field.INTERACTION_ID;
import static org.elasticsearch.inference.InferenceRequestMetadata.Field.PRODUCT_FEATURE;
import static org.elasticsearch.inference.InferenceRequestMetadata.Field.PRODUCT_SOLUTION;
import static org.elasticsearch.inference.InferenceRequestMetadata.Field.PRODUCT_USE_CASE;
import static org.elasticsearch.inference.InferenceRequestMetadata.Field.SPACE_ID;
import static org.elasticsearch.inference.InferenceRequestMetadata.Field.TRACE_ID;
import static org.elasticsearch.inference.InferenceRequestMetadata.Field.USER_ID;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class InferenceRequestMetadataTests extends ESTestCase {

    public void testFieldCatalogHasTheExpectedHeaderNames() {
        assertThat(PRODUCT_USE_CASE.httpHeader(), equalTo("X-elastic-product-use-case"));
        assertThat(PRODUCT_USE_CASE.xContentName(), equalTo("product_use_case"));
        assertThat(PRODUCT_USE_CASE.allowsMultipleRestValues(), equalTo(true));

        assertThat(PRODUCT_SOLUTION.httpHeader(), equalTo("X-elastic-product-solution"));
        assertThat(PRODUCT_SOLUTION.xContentName(), equalTo("product_solution"));
        assertThat(PRODUCT_SOLUTION.allowsMultipleRestValues(), equalTo(false));

        assertThat(PRODUCT_FEATURE.httpHeader(), equalTo("X-elastic-product-feature"));
        assertThat(PRODUCT_FEATURE.xContentName(), equalTo("product_feature"));
        assertThat(PRODUCT_FEATURE.allowsMultipleRestValues(), equalTo(false));

        assertThat(INTERACTION_ID.httpHeader(), equalTo("X-Elastic-Inference-Interaction-Id"));
        assertThat(INTERACTION_ID.xContentName(), equalTo("interaction_id"));
        assertThat(INTERACTION_ID.allowsMultipleRestValues(), equalTo(false));

        assertThat(TRACE_ID.httpHeader(), equalTo("X-Elastic-Trace-Id"));
        assertThat(TRACE_ID.xContentName(), equalTo("trace_id"));
        assertThat(TRACE_ID.allowsMultipleRestValues(), equalTo(false));

        assertThat(USER_ID.httpHeader(), equalTo("X-Elastic-User-Id"));
        assertThat(USER_ID.xContentName(), equalTo("user_id"));
        assertThat(USER_ID.allowsMultipleRestValues(), equalTo(false));

        assertThat(SPACE_ID.httpHeader(), equalTo("X-Elastic-Space-Id"));
        assertThat(SPACE_ID.xContentName(), equalTo("space_id"));
        assertThat(SPACE_ID.allowsMultipleRestValues(), equalTo(false));
    }

    public void testEmptyOmitsEveryField() {
        assertThat(InferenceRequestMetadata.EMPTY.get(PRODUCT_USE_CASE), nullValue());
        var seen = new EnumMap<InferenceRequestMetadata.Field, String>(InferenceRequestMetadata.Field.class);
        InferenceRequestMetadata.EMPTY.forEachPresent(seen::put);
        assertThat(seen, anEmptyMap());
    }

    public void testBuilderOmitsNullAndEmptyAndKeepsExactValues() {
        var metadata = InferenceRequestMetadata.builder()
            .put(PRODUCT_USE_CASE, "  security ai assistant  ")
            .put(PRODUCT_SOLUTION, "")
            .put(PRODUCT_FEATURE, null)
            .put(INTERACTION_ID, "interaction-id")
            .build();

        assertThat(metadata.get(PRODUCT_USE_CASE), equalTo("  security ai assistant  "));
        assertThat(metadata.get(PRODUCT_SOLUTION), nullValue());
        assertThat(metadata.get(PRODUCT_FEATURE), nullValue());
        assertThat(metadata.get(INTERACTION_ID), equalTo("interaction-id"));
    }

    public void testBuilderCopyIsIndependentOfLaterPuts() {
        var builder = InferenceRequestMetadata.builder().put(PRODUCT_SOLUTION, "security");
        var metadata = builder.build();
        builder.put(PRODUCT_SOLUTION, "observability");

        assertThat(metadata.get(PRODUCT_SOLUTION), equalTo("security"));
    }

    public void testCaptureReadsEachHeaderAndDropsEmpty() {
        Map<String, String> headers = Map.of(
            "X-elastic-product-use-case",
            "ai assistant",
            "X-elastic-product-solution",
            "",
            "X-Elastic-Inference-Interaction-Id",
            "interaction-id"
        );

        var metadata = InferenceRequestMetadata.capture(headers::get);

        assertThat(metadata.get(PRODUCT_USE_CASE), equalTo("ai assistant"));
        assertThat(metadata.get(PRODUCT_SOLUTION), nullValue());
        assertThat(metadata.get(PRODUCT_FEATURE), nullValue());
        assertThat(metadata.get(INTERACTION_ID), equalTo("interaction-id"));
    }

    public void testCaptureOfNothingIsTheEmptyInstance() {
        assertThat(InferenceRequestMetadata.capture(header -> null), sameInstance(InferenceRequestMetadata.EMPTY));
        assertThat(InferenceRequestMetadata.builder().put(PRODUCT_USE_CASE, "").build(), sameInstance(InferenceRequestMetadata.EMPTY));
    }

    public void testEqualityIsByStoredValue() {
        var left = InferenceRequestMetadata.builder().put(PRODUCT_FEATURE, "attack_discovery").build();
        var right = InferenceRequestMetadata.builder().put(PRODUCT_FEATURE, "attack_discovery").build();
        var other = InferenceRequestMetadata.builder().put(PRODUCT_FEATURE, "other").build();

        assertThat(left, equalTo(right));
        assertThat(left.hashCode(), equalTo(right.hashCode()));
        assertThat(left.equals(other), equalTo(false));
    }

    public void testForEachPresentVisitsOnlyStoredValues() {
        var metadata = InferenceRequestMetadata.builder().put(PRODUCT_USE_CASE, "use").put(INTERACTION_ID, "id").build();
        var seen = new EnumMap<InferenceRequestMetadata.Field, String>(InferenceRequestMetadata.Field.class);
        metadata.forEachPresent(seen::put);

        assertThat(seen, equalTo(Map.of(PRODUCT_USE_CASE, "use", INTERACTION_ID, "id")));
    }
}
