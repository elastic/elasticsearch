/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.completion;

import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockProvider;
import org.elasticsearch.xpack.inference.services.amazonbedrock.request.completion.AmazonBedrockChatCompletionEntityFactory;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xcontent.XContentParserConfiguration.EMPTY;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockProvider.AI21LABS;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockProvider.AMAZONTITAN;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockProvider.ANTHROPIC;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockProvider.COHERE;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockProvider.META;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockProvider.MISTRAL;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AmazonBedrockChatCompletionEntityFactoryTests extends ESTestCase {
    public void testEntitiesWithoutAdditionalMessages() {
        List.of(AI21LABS, AMAZONTITAN, META).forEach(provider -> {
            var expectedTemp = randomDoubleBetween(1, 10, true);
            var expectedTopP = randomDoubleBetween(1, 10, true);

            var expectedMaxToken = randomIntBetween(1, 10);
            var expectedMessage = List.of(randomIdentifier());
            var model = model(provider, expectedTemp, expectedTopP, expectedMaxToken);

            var entity = AmazonBedrockChatCompletionEntityFactory.createEntity(model, expectedMessage);

            assertThat(entity, notNullValue());
            assertThat(entity.temperature(), equalTo(expectedTemp));
            assertThat(entity.topP(), equalTo(expectedTopP));
            assertThat(entity.maxTokenCount(), equalTo(expectedMaxToken));
            assertThat(entity.additionalModelFields(), nullValue());
            assertThat(entity.messages(), equalTo(expectedMessage));
        });
    }

    public void testWithAdditionalMessages() {
        List.of(ANTHROPIC, COHERE, MISTRAL).forEach(provider -> {
            var expectedTemp = randomDoubleBetween(1, 10, true);
            var expectedTopP = randomDoubleBetween(1, 10, true);
            var expectedMaxToken = randomIntBetween(1, 10);
            var expectedMessage = List.of(randomIdentifier());
            var expectedTopK = randomDoubleBetween(1, 10, true);
            var model = model(provider, expectedTemp, expectedTopP, expectedMaxToken, expectedTopK);

            var entity = AmazonBedrockChatCompletionEntityFactory.createEntity(model, expectedMessage);

            assertThat(entity, notNullValue());
            assertThat(entity.temperature(), equalTo(expectedTemp));
            assertThat(entity.topP(), equalTo(expectedTopP));
            assertThat(entity.maxTokenCount(), equalTo(expectedMaxToken));
            assertThat(entity.messages(), equalTo(expectedMessage));
            assertThat(entity.additionalModelFields(), notNullValue());
            assertThat(entity.additionalModelFields().size(), equalTo(1));
            try (var parser = XContentFactory.xContent(XContentType.JSON).createParser(EMPTY, entity.additionalModelFields().getFirst())) {
                var additionalModelFields = parser.map();
                assertThat((Double) additionalModelFields.get("top_k"), closeTo(expectedTopK, 0.1));
            } catch (IOException e) {
                fail(e);
            }
        });
    }

    public void testEntitiesForChatCompletion() {
        List.of(ANTHROPIC, AI21LABS, AMAZONTITAN, COHERE, META, MISTRAL).forEach(provider -> {
            var expectedModel = ANTHROPIC.name();
            var expectedMaxToken = randomLongBetween(1, 10);
            var expectedStop = List.of("stop");
            var expectedTemp = randomDoubleBetween(1, 10, true);
            var expectedTopP = randomDoubleBetween(1, 10, true);

            var content = new UnifiedCompletionRequest.ContentString("content");
            var toolCall = new UnifiedCompletionRequest.ToolCall(
                "id",
                new UnifiedCompletionRequest.ToolCall.FunctionField("function", expectedModel),
                ""
            );
            var message = new UnifiedCompletionRequest.Message(content, "user", "tooluse_Z7IP83_eTt2y_TECni1ULw", List.of(toolCall));
            var expectedMessages = List.of(message);

            var expectedToolChoice = new UnifiedCompletionRequest.ToolChoiceString("any");
            var tools = List.of(new UnifiedCompletionRequest.Tool("type", null));

            var request = new UnifiedCompletionRequest(
                expectedMessages,
                expectedModel,
                expectedMaxToken,
                expectedStop,
                (float) expectedTemp,
                expectedToolChoice,
                tools,
                (float) expectedTopP
            );
            var model = model(provider, expectedTemp, expectedTopP, (int) expectedMaxToken);

            var entity = AmazonBedrockChatCompletionEntityFactory.createEntity(model, request);

            assertThat(entity, notNullValue());
            assertThat(entity.messages(), equalTo(expectedMessages));
            assertThat(entity.model(), equalTo(expectedModel));
            assertThat(entity.maxCompletionTokens(), equalTo(expectedMaxToken));
            assertThat(entity.stop(), equalTo(expectedStop));
            assertThat(entity.temperature(), equalTo((float) expectedTemp));
            assertThat(entity.toolChoice(), equalTo(expectedToolChoice));
            assertThat(entity.topP(), equalTo((float) expectedTopP));
        });
    }

    AmazonBedrockChatCompletionModel model(AmazonBedrockProvider provider, Double temperature, Double topP, Integer maxTokenCount) {
        return model(provider, temperature, topP, maxTokenCount, null);
    }

    AmazonBedrockChatCompletionModel model(AmazonBedrockProvider provider, Double temp, Double topP, Integer tokenCount, Double topK) {
        var serviceSettings = mock(AmazonBedrockChatCompletionServiceSettings.class);
        when(serviceSettings.provider()).thenReturn(provider);

        var taskSettings = mock(AmazonBedrockCompletionTaskSettings.class);
        when(taskSettings.temperature()).thenReturn(temp);
        when(taskSettings.topP()).thenReturn(topP);
        when(taskSettings.maxNewTokens()).thenReturn(tokenCount);
        when(taskSettings.topK()).thenReturn(topK);

        var model = mock(AmazonBedrockChatCompletionModel.class);
        when(model.getServiceSettings()).thenReturn(serviceSettings);
        when(model.getTaskSettings()).thenReturn(taskSettings);
        return model;
    }
}
