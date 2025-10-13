/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.completion;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.MatcherAssert;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.MAX_NEW_TOKENS_FIELD;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.TEMPERATURE_FIELD;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.TOP_K_FIELD;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.TOP_P_FIELD;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class AmazonBedrockChatCompletionRequestTaskSettingsTests extends ESTestCase {
    public void testFromMap_ReturnsEmptySettings_WhenTheMapIsEmpty() {
        var settings = AmazonBedrockChatCompletionRequestTaskSettings.fromMap(new HashMap<>(Map.of()));
        assertThat(settings, is(AmazonBedrockChatCompletionRequestTaskSettings.EMPTY_SETTINGS));
    }

    public void testFromMap_ReturnsEmptySettings_WhenTheMapDoesNotContainTheFields() {
        var settings = AmazonBedrockChatCompletionRequestTaskSettings.fromMap(new HashMap<>(Map.of("key", "model")));
        assertThat(settings, is(AmazonBedrockChatCompletionRequestTaskSettings.EMPTY_SETTINGS));
    }

    public void testFromMap_ReturnsTemperature() {
        var settings = AmazonBedrockChatCompletionRequestTaskSettings.fromMap(new HashMap<>(Map.of(TEMPERATURE_FIELD, 0.1)));
        assertThat(settings.temperature(), is(0.1));
    }

    public void testFromMap_ReturnsTopP() {
        var settings = AmazonBedrockChatCompletionRequestTaskSettings.fromMap(new HashMap<>(Map.of(TOP_P_FIELD, 0.1)));
        assertThat(settings.topP(), is(0.1));
    }

    public void testFromMap_ReturnsDoSample() {
        var settings = AmazonBedrockChatCompletionRequestTaskSettings.fromMap(new HashMap<>(Map.of(TOP_K_FIELD, 0.3)));
        assertThat(settings.topK(), is(0.3));
    }

    public void testFromMap_ReturnsMaxNewTokens() {
        var settings = AmazonBedrockChatCompletionRequestTaskSettings.fromMap(new HashMap<>(Map.of(MAX_NEW_TOKENS_FIELD, 512)));
        assertThat(settings.maxNewTokens(), is(512));
    }

    public void testFromMap_TemperatureIsInvalidValue_ThrowsValidationException() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> AmazonBedrockChatCompletionRequestTaskSettings.fromMap(new HashMap<>(Map.of(TEMPERATURE_FIELD, "invalid")))
        );

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format("field [temperature] is not of the expected type. The value [invalid] cannot be converted to a [Double]")
            )
        );
    }

    public void testFromMap_TopPIsInvalidValue_ThrowsValidationException() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> AmazonBedrockChatCompletionRequestTaskSettings.fromMap(new HashMap<>(Map.of(TOP_P_FIELD, "invalid")))
        );

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format("field [top_p] is not of the expected type. The value [invalid] cannot be converted to a [Double]")
            )
        );
    }

    public void testFromMap_TopKIsInvalidValue_ThrowsValidationException() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> AmazonBedrockChatCompletionRequestTaskSettings.fromMap(new HashMap<>(Map.of(TOP_K_FIELD, "invalid")))
        );

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            containsString("field [top_k] is not of the expected type. The value [invalid] cannot be converted to a [Double]")
        );
    }

    public void testFromMap_MaxTokensIsInvalidValue_ThrowsStatusException() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> AmazonBedrockChatCompletionRequestTaskSettings.fromMap(new HashMap<>(Map.of(MAX_NEW_TOKENS_FIELD, "invalid")))
        );

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            containsString("field [max_new_tokens] is not of the expected type. The value [invalid] cannot be converted to a [Integer]")
        );
    }
}
