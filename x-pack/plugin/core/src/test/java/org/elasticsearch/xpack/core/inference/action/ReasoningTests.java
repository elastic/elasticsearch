/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.completion.Reasoning;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.EFFORT_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.ENABLE_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.EXCLUDE_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.MAX_TOKENS_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.SUMMARY_FIELD;
import static org.hamcrest.Matchers.is;

public class ReasoningTests extends AbstractBWCWireSerializationTestCase<Reasoning> {

    public void testParsingReasoning_AllFields_WithEffort() throws IOException {
        String requestJson = """
            {
                "effort": "medium",
                "summary": "detailed",
                "exclude": false,
                "enable": false
            }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var reasoning = Reasoning.PARSER.apply(parser, null);
            var expected = new Reasoning(Reasoning.ReasoningEffort.MEDIUM, null, Reasoning.ReasoningSummary.DETAILED, false, false);

            assertThat(reasoning, is(expected));
        }
    }

    public void testParsingReasoning_OnlyEffort() throws IOException {
        String requestJson = """
            {
                "effort": "medium"
            }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var reasoning = Reasoning.PARSER.apply(parser, null);
            var expected = new Reasoning(Reasoning.ReasoningEffort.MEDIUM, null, null, null, null);

            assertThat(reasoning, is(expected));
        }
    }

    public void testParsingReasoning_OnlyMaxTokens() throws IOException {
        String requestJson = """
            {
                "max_tokens": 25
            }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var reasoning = Reasoning.PARSER.apply(parser, null);
            var expected = new Reasoning(null, 25L, null, null, null);

            assertThat(reasoning, is(expected));
        }
    }

    public void testParsingReasoning_OnlyEnable() throws IOException {
        String requestJson = """
            {
                "enable": true
            }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var reasoning = Reasoning.PARSER.apply(parser, null);
            var expected = new Reasoning(null, null, null, null, true);

            assertThat(reasoning, is(expected));
        }
    }

    public void testParsingReasoning_BothEffortAndMaxTokens_ThrowsException() throws IOException {
        String requestJson = """
            {
                "effort": "medium",
                "max_tokens": 25
            }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var exception = assertThrows(IllegalArgumentException.class, () -> Reasoning.PARSER.apply(parser, null));
            assertThat(exception.getMessage(), is("The following fields are not allowed together: [effort, max_tokens] "));
        }
    }

    public void testParsingReasoning_OnlyEnableFalse_ThrowsException() throws IOException {
        String requestJson = """
            {
                "enable": false
            }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var exception = assertThrows(XContentParseException.class, () -> Reasoning.PARSER.apply(parser, null));
            ElasticsearchStatusException rootCause = (ElasticsearchStatusException) ExceptionsHelper.unwrap(
                exception,
                ElasticsearchStatusException.class
            );
            assertThat(rootCause.getMessage(), is("Either [effort] or [max_tokens] must not be null, or [enable] must be true."));
            assertThat(rootCause.status(), is(RestStatus.BAD_REQUEST));
        }
    }

    public void testParsingReasoning_NoRequiredFields_ThrowsException() throws IOException {
        String requestJson = "{}";

        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var exception = assertThrows(IllegalArgumentException.class, () -> Reasoning.PARSER.apply(parser, null));
            assertThat(exception.getMessage(), is("Required one of fields [effort, max_tokens, enable], but none were specified."));
        }
    }

    public void testParsingReasoning_UnsupportedEffortValue_ThrowsException() throws IOException {
        String requestJson = """
            {
                "effort": "unknown"
            }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var exception = assertThrows(XContentParseException.class, () -> Reasoning.PARSER.apply(parser, null));
            ElasticsearchStatusException rootCause = (ElasticsearchStatusException) ExceptionsHelper.unwrap(
                exception,
                ElasticsearchStatusException.class
            );
            assertThat(
                rootCause.getMessage(),
                is("Unrecognized type [unknown] in object [effort], must be one of [xhigh, high, medium, low, minimal, none]")
            );
            assertThat(rootCause.status(), is(RestStatus.BAD_REQUEST));
        }
    }

    public void testParsingReasoning_UnsupportedSummaryValue_ThrowsException() throws IOException {
        String requestJson = """
            {
                "effort": "medium",
                "summary": "unknown"
            }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var exception = assertThrows(XContentParseException.class, () -> Reasoning.PARSER.apply(parser, null));
            ElasticsearchStatusException rootCause = (ElasticsearchStatusException) ExceptionsHelper.unwrap(
                exception,
                ElasticsearchStatusException.class
            );
            assertThat(
                rootCause.getMessage(),
                is("Unrecognized type [unknown] in object [summary], must be one of [auto, concise, detailed]")
            );
            assertThat(rootCause.status(), is(RestStatus.BAD_REQUEST));
        }
    }

    @Override
    protected Reasoning mutateInstanceForVersion(Reasoning instance, TransportVersion version) {
        // checks for version compatibility are done outside of Reasoning class, so we can return the instance as is without mutation
        return instance;
    }

    @Override
    protected Writeable.Reader<Reasoning> instanceReader() {
        return Reasoning::new;
    }

    @Override
    protected Reasoning createTestInstance() {
        return randomReasoning();
    }

    /**
     * Mutates a single eligible field of the Reasoning instance, ensuring business rules are respected.
     * Eligible fields are determined so that mutation will not violate Reasoning validation logic.
     * Only one field is mutated per call.
     */
    @Override
    protected Reasoning mutateInstance(Reasoning instance) throws IOException {
        Reasoning.ReasoningEffort effort = instance.effort();
        Long maxTokens = instance.maxTokens();
        Reasoning.ReasoningSummary summary = instance.summary();
        Boolean exclude = instance.exclude();
        Boolean enable = instance.enable();

        // Build eligible fields for mutation following the validation rules of Reasoning class.
        // This prevents mutation of fields that would lead to an invalid Reasoning instance.
        var eligibleFields = buildEligibleFields(effort, maxTokens);

        // Randomly select one eligible field to mutate.
        switch (randomFrom(eligibleFields)) {
            case EFFORT_FIELD -> effort = randomValueOtherThan(effort, () -> randomFrom(Reasoning.ReasoningEffort.values()));
            case MAX_TOKENS_FIELD -> maxTokens = randomValueOtherThan(maxTokens, ESTestCase::randomNonNegativeLong);
            case SUMMARY_FIELD -> summary = randomValueOtherThan(
                summary,
                () -> randomBoolean() ? randomFrom(Reasoning.ReasoningSummary.values()) : null
            );
            case EXCLUDE_FIELD -> exclude = randomValueOtherThan(exclude, ESTestCase::randomOptionalBoolean);
            case ENABLE_FIELD -> enable = randomValueOtherThan(enable, ESTestCase::randomOptionalBoolean);
            default -> throw new AssertionError("Illegal mutation branch");
        }
        // Return new Reasoning instance. Business rules are enforced by eligible field selection.
        return new Reasoning(effort, maxTokens, summary, exclude, enable);
    }

    private static Set<String> buildEligibleFields(Reasoning.ReasoningEffort effort, Long maxTokens) {
        var eligibleFields = new HashSet<String>(5);
        // Summary and exclude are always eligible for mutation
        eligibleFields.add(SUMMARY_FIELD);
        eligibleFields.add(EXCLUDE_FIELD);
        // Only mutate effort if present
        if (effort != null) {
            eligibleFields.add(EFFORT_FIELD);
        }
        // Only mutate maxTokens if present
        if (maxTokens != null) {
            eligibleFields.add(MAX_TOKENS_FIELD);
        }
        // Only mutate enable if effort or maxTokens is present
        if (effort != null || maxTokens != null) {
            eligibleFields.add(ENABLE_FIELD);
        }
        return eligibleFields;
    }

    static Reasoning randomReasoning() {
        var effort = randomBoolean() ? randomFrom(Reasoning.ReasoningEffort.values()) : null;
        var maxTokens = (effort == null && randomBoolean()) ? randomNonNegativeLong() : null;
        Boolean enable;
        if (effort == null && maxTokens == null) {
            enable = true;
        } else {
            enable = randomOptionalBoolean();
        }
        return new Reasoning(
            effort,
            maxTokens,
            randomBoolean() ? randomFrom(Reasoning.ReasoningSummary.values()) : null,
            randomOptionalBoolean(),
            enable
        );
    }
}
