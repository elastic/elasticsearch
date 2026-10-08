/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.client;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.bedrockruntime.model.BedrockRuntimeException;
import software.amazon.awssdk.services.bedrockruntime.model.CitationsDelta;
import software.amazon.awssdk.services.bedrockruntime.model.ContentBlockDelta;
import software.amazon.awssdk.services.bedrockruntime.model.ContentBlockDeltaEvent;
import software.amazon.awssdk.services.bedrockruntime.model.ContentBlockStart;
import software.amazon.awssdk.services.bedrockruntime.model.ContentBlockStartEvent;
import software.amazon.awssdk.services.bedrockruntime.model.ContentBlockStopEvent;
import software.amazon.awssdk.services.bedrockruntime.model.ConverseStreamMetadataEvent;
import software.amazon.awssdk.services.bedrockruntime.model.ConverseStreamOutput;
import software.amazon.awssdk.services.bedrockruntime.model.ConverseStreamResponseHandler;
import software.amazon.awssdk.services.bedrockruntime.model.MessageStartEvent;
import software.amazon.awssdk.services.bedrockruntime.model.MessageStopEvent;
import software.amazon.awssdk.services.bedrockruntime.model.ReasoningContentBlockDelta;
import software.amazon.awssdk.services.bedrockruntime.model.TokenUsage;
import software.amazon.awssdk.services.bedrockruntime.model.ToolUseBlockDelta;
import software.amazon.awssdk.services.bedrockruntime.model.ToolUseBlockStart;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.inference.completion.ReasoningDetail;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.results.StreamingUnifiedChatCompletionResults;
import org.elasticsearch.xpack.core.inference.results.completion.ChatCompletionMessageResponse;
import org.elasticsearch.xpack.core.inference.results.completion.ChatCompletionUsageResponse;
import org.elasticsearch.xpack.core.inference.results.completion.ChatCompletionUsageResponse.PromptTokensDetails;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockProvider;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Flow;

import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;
import static org.elasticsearch.xpack.inference.services.anthropic.AnthropicChatCompletionStreamingProcessor.ANTHROPIC_CLAUDE_V1_FORMAT;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.assertArg;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static software.amazon.awssdk.services.bedrockruntime.model.StopReason.TOOL_USE;

public class AmazonBedrockChatCompletionStreamingProcessorTests extends ESTestCase {
    private AmazonBedrockChatCompletionStreamingProcessor processor;

    @Before
    public void createProcessor() throws Exception {
        processor = createProcessor(randomFrom(AmazonBedrockProvider.values()));
    }

    private static AmazonBedrockChatCompletionStreamingProcessor createProcessor(AmazonBedrockProvider provider) {
        ThreadPool threadPool = mock();
        when(threadPool.executor(UTILITY_THREAD_POOL_NAME)).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        return new AmazonBedrockChatCompletionStreamingProcessor(threadPool, "model", provider);
    }

    /**
     * We do not issue requests on subscribe because the downstream will control the pacing.
     */
    public void testOnSubscribeBeforeDownstreamDoesNotRequest() {
        var upstream = mock(Flow.Subscription.class);
        processor.onSubscribe(upstream);

        verify(upstream, never()).request(anyLong());
    }

    /**
     * If the downstream requests data before the upstream is set, when the upstream is set, we will forward the pending requests to it.
     */
    public void testOnSubscribeAfterDownstreamRequests() {
        var expectedRequestCount = randomLongBetween(1, 500);
        Flow.Subscriber<StreamingUnifiedChatCompletionResults.Results> subscriber = mock();
        doAnswer(ans -> {
            Flow.Subscription sub = ans.getArgument(0);
            sub.request(expectedRequestCount);
            return null;
        }).when(subscriber).onSubscribe(any());
        processor.subscribe(subscriber);

        var upstream = mock(Flow.Subscription.class);
        processor.onSubscribe(upstream);

        verify(upstream, times(1)).request(anyLong());
    }

    public void testCancelDuplicateSubscriptions() {
        processor.onSubscribe(mock());

        var upstream = mock(Flow.Subscription.class);
        processor.onSubscribe(upstream);

        verify(upstream, times(1)).cancel();
        verifyNoMoreInteractions(upstream);
    }

    public void testMultiplePublishesCallsOnError() {
        processor.subscribe(mock());

        Flow.Subscriber<StreamingUnifiedChatCompletionResults.Results> subscriber = mock();
        processor.subscribe(subscriber);

        verify(subscriber, times(1)).onError(assertArg(e -> {
            assertThat(e, isA(IllegalStateException.class));
            assertThat(e.getMessage(), equalTo("Subscriber already set."));
        }));
    }

    public void testForwardsDownstream() {
        var expectedMessageStartRole = "assistant";
        ExecutorService executorService = mock();
        ThreadPool threadPool = mock();
        when(threadPool.executor(UTILITY_THREAD_POOL_NAME)).thenReturn(executorService);
        processor = new AmazonBedrockChatCompletionStreamingProcessor(threadPool, "model", randomFrom(AmazonBedrockProvider.values()));
        doAnswer(ans -> {
            Runnable command = ans.getArgument(0);
            command.run();
            return null;
        }).when(executorService).execute(any());

        Flow.Subscription upstream = mock();
        processor.onSubscribe(upstream);
        Flow.Subscriber<StreamingUnifiedChatCompletionResults.Results> downstream = mock();
        processor.subscribe(downstream);

        ConverseStreamOutput messageStartOutput = messageStartOutput(expectedMessageStartRole);
        ConverseStreamOutput contentBlockStartOutput = contentBlockStartOutput();
        ConverseStreamOutput contentBlockDeltaOutput = contentBlockDeltaOutput();
        ConverseStreamOutput contentBlockStopOutput = contentBlockStopOutput();
        ConverseStreamOutput messageStopOutput = messageStopOutput();
        ConverseStreamOutput metadata = metadataOutput();

        processor.onNext(messageStartOutput);
        processor.onNext(contentBlockStartOutput);
        processor.onNext(contentBlockDeltaOutput);

        ArgumentCaptor<StreamingUnifiedChatCompletionResults.Results> argument = ArgumentCaptor.forClass(
            StreamingUnifiedChatCompletionResults.Results.class
        );

        // 3 because we call onNext three times above
        var initialInvocations = 3;

        verify(downstream, times(initialInvocations)).onNext(argument.capture());
        assertThat(argument.getAllValues().size(), is(3));
        assertThat(argument.getAllValues().get(0).chunks().size(), is(1));
        assertThat(
            argument.getAllValues().get(0).chunks().getFirst().choices().getFirst().message().role(),
            equalTo(expectedMessageStartRole)
        );

        assertThat(argument.getAllValues().get(1).chunks().size(), is(1));
        assertThat(argument.getAllValues().get(2).chunks().size(), is(1));

        verify(executorService, times(3)).execute(any());
        verify(upstream, times(0)).request(anyLong());

        // These event types are ignored, so it won't call onNext for the downstream
        processor.onNext(contentBlockStopOutput);
        processor.onNext(contentBlockStopOutput);

        // These cause actual calls to onNext for the downstream
        processor.onNext(messageStopOutput);
        processor.onNext(metadata);

        // Only 2 calls because content block stop is ignored
        verify(downstream, times(initialInvocations + 2)).onNext(any());
        assertThat(argument.getAllValues().size(), is(3));
        assertThat(argument.getAllValues().get(0).chunks().size(), is(1));
        assertThat(argument.getAllValues().get(1).chunks().size(), is(1));
        assertThat(argument.getAllValues().get(2).chunks().size(), is(1));
    }

    public void testAnthropicReasoningDeltasEmitReasoningAndDetails() {
        processor = createProcessor(AmazonBedrockProvider.ANTHROPIC);
        var redacted = "redacted".getBytes(StandardCharsets.UTF_8);

        var messages = messagesFrom(
            contentBlockDeltaOutput(ContentBlockDelta.fromReasoningContent(ReasoningContentBlockDelta.fromText("thinking")), 0),
            contentBlockDeltaOutput(ContentBlockDelta.fromReasoningContent(ReasoningContentBlockDelta.fromSignature("sig")), 0),
            contentBlockDeltaOutput(
                ContentBlockDelta.fromReasoningContent(ReasoningContentBlockDelta.fromRedactedContent(SdkBytes.fromByteArray(redacted))),
                1
            ),
            contentBlockDeltaOutput(ContentBlockDelta.fromText("answer"), 2)
        );

        assertThat(messages.size(), is(4));
        assertThat(messages.get(0).reasoning(), equalTo("thinking"));
        assertThat(
            messages.get(0).reasoningDetails(),
            equalTo(List.of(new ReasoningDetail.TextReasoningDetail(ANTHROPIC_CLAUDE_V1_FORMAT, null, 0L, "thinking", null)))
        );
        assertNull(messages.get(1).reasoning());
        assertThat(
            messages.get(1).reasoningDetails(),
            equalTo(List.of(new ReasoningDetail.TextReasoningDetail(ANTHROPIC_CLAUDE_V1_FORMAT, null, 0L, null, "sig")))
        );
        assertThat(
            messages.get(2).reasoningDetails(),
            equalTo(
                List.of(
                    new ReasoningDetail.EncryptedReasoningDetail(
                        ANTHROPIC_CLAUDE_V1_FORMAT,
                        null,
                        1L,
                        Base64.getEncoder().encodeToString(redacted)
                    )
                )
            )
        );
        assertThat(messages.get(3).content(), equalTo("answer"));
        assertNull(messages.get(3).reasoning());
        assertNull(messages.get(3).reasoningDetails());
    }

    public void testNonAnthropicReasoningDeltasEmitReasoningTextOnly() {
        processor = createProcessor(
            randomValueOtherThan(AmazonBedrockProvider.ANTHROPIC, () -> randomFrom(AmazonBedrockProvider.values()))
        );

        var messages = messagesFrom(
            contentBlockDeltaOutput(ContentBlockDelta.fromReasoningContent(ReasoningContentBlockDelta.fromText("thinking")), 0),
            contentBlockDeltaOutput(ContentBlockDelta.fromReasoningContent(ReasoningContentBlockDelta.fromSignature("sig")), 0),
            contentBlockDeltaOutput(
                ContentBlockDelta.fromReasoningContent(ReasoningContentBlockDelta.fromRedactedContent(SdkBytes.fromUtf8String("redacted"))),
                1
            )
        );

        assertThat(messages.size(), is(1));
        assertThat(messages.get(0).reasoning(), equalTo("thinking"));
        assertNull(messages.get(0).reasoningDetails());
    }

    public void testUnknownStreamMembersAreSkipped() {
        processor = createProcessor(AmazonBedrockProvider.ANTHROPIC);

        var messages = messagesFrom(
            contentBlockDeltaOutput(ContentBlockDelta.fromCitation(CitationsDelta.builder().build()), 0),
            contentBlockDeltaOutput(ContentBlockDelta.builder().build(), 0),
            contentBlockDeltaOutput(ContentBlockDelta.fromReasoningContent(ReasoningContentBlockDelta.builder().build()), 0),
            contentBlockStartOutput(ContentBlockStart.builder().build(), 0)
        );

        assertThat(messages.size(), is(0));
    }

    public void testReasoningTextAndParallelToolCallsShareChoiceZero() {
        processor = createProcessor(AmazonBedrockProvider.ANTHROPIC);

        var messages = messagesFrom(
            contentBlockDeltaOutput(ContentBlockDelta.fromReasoningContent(ReasoningContentBlockDelta.fromText("thinking")), 0),
            contentBlockDeltaOutput(ContentBlockDelta.fromText("answer"), 1),
            toolUseStartOutput("call-a", "first", 2),
            toolUseDeltaOutput("{\"a\":1}", 2),
            toolUseStartOutput("call-b", "second", 3),
            toolUseDeltaOutput("{\"b\":2}", 3)
        );

        assertThat(messages.size(), is(6));
        assertThat(messages.get(0).reasoning(), equalTo("thinking"));
        assertThat(messages.get(1).content(), equalTo("answer"));
        assertThat(
            messages.subList(2, 6).stream().map(message -> message.toolCalls().getFirst().index()).toList(),
            equalTo(List.of(0, 0, 1, 1))
        );
        assertThat(
            messages.subList(2, 6).stream().map(message -> message.toolCalls().getFirst().id()).toList(),
            equalTo(Arrays.asList("call-a", null, "call-b", null))
        );
        assertThat(messages.get(2).toolCalls().getFirst().function().name(), equalTo("first"));
        assertThat(messages.get(3).toolCalls().getFirst().function().arguments(), equalTo("{\"a\":1}"));
        assertThat(messages.get(4).toolCalls().getFirst().function().name(), equalTo("second"));
        assertThat(messages.get(5).toolCalls().getFirst().function().arguments(), equalTo("{\"b\":2}"));
    }

    /**
     * Models that omit thinking text from the response stream only the signature for the reasoning block.
     */
    public void testSignatureOnlyReasoningThenAnswer() {
        processor = createProcessor(AmazonBedrockProvider.ANTHROPIC);

        var messages = messagesFrom(
            contentBlockDeltaOutput(ContentBlockDelta.fromReasoningContent(ReasoningContentBlockDelta.fromSignature("sig")), 0),
            contentBlockDeltaOutput(ContentBlockDelta.fromText("answer"), 1)
        );

        assertThat(messages.size(), is(2));
        assertNull(messages.get(0).reasoning());
        assertThat(
            messages.get(0).reasoningDetails(),
            equalTo(List.of(new ReasoningDetail.TextReasoningDetail(ANTHROPIC_CLAUDE_V1_FORMAT, null, 0L, null, "sig")))
        );
        assertThat(messages.get(1).content(), equalTo("answer"));
        assertNull(messages.get(1).reasoningDetails());
    }

    public void testRepeatedSignatureFragmentsShareReasoningIndex() {
        processor = createProcessor(AmazonBedrockProvider.ANTHROPIC);

        var messages = messagesFrom(
            contentBlockDeltaOutput(ContentBlockDelta.fromReasoningContent(ReasoningContentBlockDelta.fromSignature("sig-a")), 0),
            contentBlockDeltaOutput(ContentBlockDelta.fromReasoningContent(ReasoningContentBlockDelta.fromSignature("sig-b")), 0)
        );

        assertThat(
            messages.stream().map(ChatCompletionMessageResponse::reasoningDetails).toList(),
            equalTo(
                List.of(
                    List.of(new ReasoningDetail.TextReasoningDetail(ANTHROPIC_CLAUDE_V1_FORMAT, null, 0L, null, "sig-a")),
                    List.of(new ReasoningDetail.TextReasoningDetail(ANTHROPIC_CLAUDE_V1_FORMAT, null, 0L, null, "sig-b"))
                )
            )
        );
    }

    public void testToolUseDeltaWithoutStartIsSkipped() {
        var messages = messagesFrom(toolUseDeltaOutput("{}", 1));

        assertThat(messages.size(), is(0));
    }

    public void testErrorAfterSkippedEventIsDelivered() {
        var upstream = mock(Flow.Subscription.class);
        var downstream = subscribedDownstream(upstream);
        var expectedError = BedrockRuntimeException.builder().message("ahhhhhh").build();

        processor.onNext(skippedDeltaOutput());
        verify(upstream, times(2)).request(1);
        processor.onError(expectedError);

        verify(downstream, times(1)).onError(same(expectedError));
        verify(downstream, never()).onComplete();
    }

    public void testCompletionAfterSkippedEventIsDelivered() {
        var upstream = mock(Flow.Subscription.class);
        var downstream = subscribedDownstream(upstream);

        processor.onNext(skippedDeltaOutput());
        processor.onComplete();

        verify(downstream, times(1)).onComplete();
        verify(downstream, never()).onError(any());
    }

    public void testErrorAfterSkippedEventAndBlockStopIsDelivered() {
        var upstream = mock(Flow.Subscription.class);
        var downstream = subscribedDownstream(upstream);
        var expectedError = BedrockRuntimeException.builder().message("ahhhhhh").build();

        processor.onNext(skippedDeltaOutput());
        processor.onNext(contentBlockStopOutput());
        verify(upstream, times(3)).request(1);
        processor.onError(expectedError);

        verify(downstream, times(1)).onError(same(expectedError));
        verify(downstream, never()).onComplete();
    }

    /**
     * Subscribes a downstream that requests one item, the way the SSE listener does.
     */
    private Flow.Subscriber<StreamingUnifiedChatCompletionResults.Results> subscribedDownstream(Flow.Subscription upstream) {
        processor.onSubscribe(upstream);
        Flow.Subscriber<StreamingUnifiedChatCompletionResults.Results> downstream = mock();
        doAnswer(ans -> {
            Flow.Subscription subscription = ans.getArgument(0);
            subscription.request(1);
            return null;
        }).when(downstream).onSubscribe(any());
        processor.subscribe(downstream);
        verify(upstream).request(1);
        return downstream;
    }

    private ConverseStreamOutput skippedDeltaOutput() {
        return contentBlockDeltaOutput(ContentBlockDelta.fromCitation(CitationsDelta.builder().build()), 0);
    }

    /**
     * Sends each output through the processor and returns the message of every chunk sent downstream.
     */
    private List<ChatCompletionMessageResponse> messagesFrom(ConverseStreamOutput... outputs) {
        Flow.Subscription upstream = mock();
        processor.onSubscribe(upstream);
        Flow.Subscriber<StreamingUnifiedChatCompletionResults.Results> downstream = mock();
        processor.subscribe(downstream);

        for (var output : outputs) {
            processor.onNext(output);
        }

        verify(downstream, never()).onError(any());
        ArgumentCaptor<StreamingUnifiedChatCompletionResults.Results> argument = ArgumentCaptor.forClass(
            StreamingUnifiedChatCompletionResults.Results.class
        );
        verify(downstream, Mockito.atLeast(0)).onNext(argument.capture());
        // Every skipped output asks upstream for the next item instead of sending a chunk downstream.
        verify(upstream, times(outputs.length - argument.getAllValues().size())).request(1);
        var chunks = argument.getAllValues().stream().flatMap(results -> results.chunks().stream()).toList();
        for (var chunk : chunks) {
            for (var choice : chunk.choices()) {
                assertThat(choice.index(), is(0));
            }
        }
        return chunks.stream().map(chunk -> chunk.choices().getFirst().message()).toList();
    }

    private ConverseStreamOutput toolUseStartOutput(String id, String name, int contentBlockIndex) {
        return contentBlockStartOutput(
            ContentBlockStart.fromToolUse(ToolUseBlockStart.builder().toolUseId(id).name(name).build()),
            contentBlockIndex
        );
    }

    private ConverseStreamOutput toolUseDeltaOutput(String input, int contentBlockIndex) {
        return contentBlockDeltaOutput(ContentBlockDelta.fromToolUse(ToolUseBlockDelta.builder().input(input).build()), contentBlockIndex);
    }

    private ConverseStreamOutput contentBlockDeltaOutput(ContentBlockDelta delta, int contentBlockIndex) {
        ConverseStreamOutput output = mock();
        when(output.sdkEventType()).thenReturn(ConverseStreamOutput.EventType.CONTENT_BLOCK_DELTA);
        doAnswer(ans -> {
            ConverseStreamResponseHandler.Visitor visitor = ans.getArgument(0);
            visitor.visitContentBlockDelta(ContentBlockDeltaEvent.builder().delta(delta).contentBlockIndex(contentBlockIndex).build());
            return null;
        }).when(output).accept(any());
        return output;
    }

    private ConverseStreamOutput contentBlockStartOutput(ContentBlockStart start, int contentBlockIndex) {
        ConverseStreamOutput output = mock();
        when(output.sdkEventType()).thenReturn(ConverseStreamOutput.EventType.CONTENT_BLOCK_START);
        doAnswer(ans -> {
            ConverseStreamResponseHandler.Visitor visitor = ans.getArgument(0);
            visitor.visitContentBlockStart(ContentBlockStartEvent.builder().start(start).contentBlockIndex(contentBlockIndex).build());
            return null;
        }).when(output).accept(any());
        return output;
    }

    private ConverseStreamOutput messageStartOutput(String role) {
        ConverseStreamOutput output = mock();
        when(output.sdkEventType()).thenReturn(ConverseStreamOutput.EventType.MESSAGE_START);
        doAnswer(ans -> {
            ConverseStreamResponseHandler.Visitor visitor = ans.getArgument(0);
            MessageStartEvent event = MessageStartEvent.builder().role(role).build();
            visitor.visitMessageStart(event);
            return null;
        }).when(output).accept(any());
        return output;
    }

    private ConverseStreamOutput contentBlockStartOutput() {
        ConverseStreamOutput output = mock();
        when(output.sdkEventType()).thenReturn(ConverseStreamOutput.EventType.CONTENT_BLOCK_START);
        doAnswer(ans -> {
            ConverseStreamResponseHandler.Visitor visitor = ans.getArgument(0);
            ContentBlockStartEvent event = ContentBlockStartEvent.builder()
                .start(ContentBlockStart.builder().toolUse(ToolUseBlockStart.builder().build()).build())
                .contentBlockIndex(0)
                .build();
            visitor.visitContentBlockStart(event);
            return null;
        }).when(output).accept(any());
        return output;
    }

    private ConverseStreamOutput contentBlockDeltaOutput() {
        ConverseStreamOutput output = mock();
        when(output.sdkEventType()).thenReturn(ConverseStreamOutput.EventType.CONTENT_BLOCK_DELTA);
        doAnswer(ans -> {
            ConverseStreamResponseHandler.Visitor visitor = ans.getArgument(0);
            ContentBlockDelta delta = ContentBlockDelta.builder().text("some text").build();
            ContentBlockDeltaEvent event = ContentBlockDeltaEvent.builder().delta(delta).contentBlockIndex(0).build();
            visitor.visitContentBlockDelta(event);
            return null;
        }).when(output).accept(any());
        return output;
    }

    private ConverseStreamOutput contentBlockStopOutput() {
        ConverseStreamOutput output = mock();
        when(output.sdkEventType()).thenReturn(ConverseStreamOutput.EventType.CONTENT_BLOCK_STOP);
        doAnswer(ans -> {
            ConverseStreamResponseHandler.Visitor visitor = ans.getArgument(0);
            ContentBlockStopEvent event = ContentBlockStopEvent.builder().contentBlockIndex(0).build();
            visitor.visitContentBlockStop(event);
            return null;
        }).when(output).accept(any());
        return output;
    }

    private ConverseStreamOutput messageStopOutput() {
        ConverseStreamOutput output = mock();
        when(output.sdkEventType()).thenReturn(ConverseStreamOutput.EventType.MESSAGE_STOP);
        doAnswer(ans -> {
            ConverseStreamResponseHandler.Visitor visitor = ans.getArgument(0);
            MessageStopEvent event = MessageStopEvent.builder().stopReason(TOOL_USE).build();
            visitor.visitMessageStop(event);
            return null;
        }).when(output).accept(any());
        return output;
    }

    private ConverseStreamOutput metadataOutput() {
        return metadataOutput(TokenUsage.builder().inputTokens(1).outputTokens(1).totalTokens(2).build());
    }

    private ConverseStreamOutput metadataOutput(TokenUsage tokenUsage) {
        ConverseStreamOutput output = mock();
        when(output.sdkEventType()).thenReturn(ConverseStreamOutput.EventType.METADATA);
        doAnswer(ans -> {
            ConverseStreamResponseHandler.Visitor visitor = ans.getArgument(0);
            ConverseStreamMetadataEvent event = ConverseStreamMetadataEvent.builder().usage(tokenUsage).build();
            visitor.visitMetadata(event);
            return null;
        }).when(output).accept(any());
        return output;
    }

    /**
     * Bedrock omits the cache token fields when prompt caching is unused; the usage must omit prompt_tokens_details
     * so the response shape matches deployments that predate cache_write_tokens support.
     */
    public void testMetadataWithoutCacheTokensOmitsPromptTokensDetails() {
        var usage = usageFromMetadataEvent(TokenUsage.builder().inputTokens(1).outputTokens(2).totalTokens(3).build());

        assertThat(usage.promptTokens(), is(1));
        assertThat(usage.completionTokens(), is(2));
        assertThat(usage.totalTokens(), is(3));
        assertNull(usage.promptTokensDetails());
    }

    public void testMetadataWithCacheTokensPopulatesPromptTokensDetails() {
        var usage = usageFromMetadataEvent(
            TokenUsage.builder().inputTokens(1).outputTokens(2).totalTokens(11).cacheReadInputTokens(3).cacheWriteInputTokens(5).build()
        );

        // prompt tokens include the bedrock input tokens plus both cache token counts
        assertThat(usage.promptTokens(), is(9));
        assertThat(usage.promptTokensDetails(), equalTo(new PromptTokensDetails(3, 5)));
    }

    private ChatCompletionUsageResponse usageFromMetadataEvent(TokenUsage tokenUsage) {
        var upstream = mock(Flow.Subscription.class);
        processor.onSubscribe(upstream);
        Flow.Subscriber<StreamingUnifiedChatCompletionResults.Results> downstream = mock();
        processor.subscribe(downstream);

        processor.onNext(metadataOutput(tokenUsage));

        ArgumentCaptor<StreamingUnifiedChatCompletionResults.Results> argument = ArgumentCaptor.forClass(
            StreamingUnifiedChatCompletionResults.Results.class
        );
        verify(downstream).onNext(argument.capture());
        return argument.getValue().chunks().getFirst().usage();
    }

    public void verifyCompleteBeforeRequest() {
        processor.onComplete();

        Flow.Subscriber<StreamingUnifiedChatCompletionResults.Results> downstream = mock();
        var sub = ArgumentCaptor.forClass(Flow.Subscription.class);
        processor.subscribe(downstream);
        verify(downstream).onSubscribe(sub.capture());

        sub.getValue().request(1);
        verify(downstream, times(1)).onComplete();
    }

    public void verifyCompleteAfterRequest() {

        Flow.Subscriber<StreamingUnifiedChatCompletionResults.Results> downstream = mock();
        var sub = ArgumentCaptor.forClass(Flow.Subscription.class);
        processor.subscribe(downstream);
        verify(downstream).onSubscribe(sub.capture());

        sub.getValue().request(1);
        processor.onComplete();
        verify(downstream, times(1)).onComplete();
    }

    public void verifyOnErrorBeforeRequest() {
        var expectedError = BedrockRuntimeException.builder().message("ahhhhhh").build();
        processor.onError(expectedError);

        Flow.Subscriber<StreamingUnifiedChatCompletionResults.Results> downstream = mock();
        var sub = ArgumentCaptor.forClass(Flow.Subscription.class);
        processor.subscribe(downstream);
        verify(downstream).onSubscribe(sub.capture());

        sub.getValue().request(1);
        verify(downstream, times(1)).onError(assertArg(e -> {
            assertThat(e, isA(ElasticsearchException.class));
            assertThat(e.getCause(), is(expectedError));
        }));
    }

    public void verifyOnErrorAfterRequest() {
        var expectedError = BedrockRuntimeException.builder().message("ahhhhhh").build();

        Flow.Subscriber<StreamingUnifiedChatCompletionResults.Results> downstream = mock();
        var sub = ArgumentCaptor.forClass(Flow.Subscription.class);
        processor.subscribe(downstream);
        verify(downstream).onSubscribe(sub.capture());

        sub.getValue().request(1);
        processor.onError(expectedError);
        verify(downstream, times(1)).onError(assertArg(e -> {
            assertThat(e, isA(ElasticsearchException.class));
            assertThat(e.getCause(), is(expectedError));
        }));
    }

    public void verifyAsyncOnCompleteIsStillDeliveredSynchronously() {
        mockUpstream();

        Flow.Subscriber<StreamingUnifiedChatCompletionResults.Results> downstream = mock();
        var sub = ArgumentCaptor.forClass(Flow.Subscription.class);
        processor.subscribe(downstream);
        verify(downstream).onSubscribe(sub.capture());

        sub.getValue().request(1);
        verify(downstream, times(1)).onNext(any());
        processor.onComplete();
        verify(downstream, times(0)).onComplete();
        sub.getValue().request(1);
        verify(downstream, times(1)).onComplete();
    }

    private void mockUpstream() {
        Flow.Subscription upstream = mock();
        doAnswer(ans -> {
            processor.onNext(messageStartOutput(randomIdentifier()));
            processor.onNext(contentBlockStartOutput());
            processor.onNext(contentBlockDeltaOutput());
            processor.onNext(contentBlockStopOutput());
            processor.onNext(messageStopOutput());
            processor.onNext(metadataOutput());
            return null;
        }).when(upstream).request(anyLong());
        processor.onSubscribe(upstream);
    }
}
