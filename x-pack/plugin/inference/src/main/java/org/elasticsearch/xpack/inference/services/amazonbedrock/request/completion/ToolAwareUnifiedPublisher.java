/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.request.completion;

import software.amazon.awssdk.core.document.Document;
import software.amazon.awssdk.services.bedrockruntime.model.ContentBlock;
import software.amazon.awssdk.services.bedrockruntime.model.ConversationRole;
import software.amazon.awssdk.services.bedrockruntime.model.ConverseStreamRequest;
import software.amazon.awssdk.services.bedrockruntime.model.Message;
import software.amazon.awssdk.services.bedrockruntime.model.SpecificToolChoice;
import software.amazon.awssdk.services.bedrockruntime.model.Tool;
import software.amazon.awssdk.services.bedrockruntime.model.ToolChoice;
import software.amazon.awssdk.services.bedrockruntime.model.ToolConfiguration;
import software.amazon.awssdk.services.bedrockruntime.model.ToolInputSchema;
import software.amazon.awssdk.services.bedrockruntime.model.ToolResultBlock;
import software.amazon.awssdk.services.bedrockruntime.model.ToolResultContentBlock;
import software.amazon.awssdk.services.bedrockruntime.model.ToolSpecification;

import org.elasticsearch.xpack.core.inference.results.StreamingUnifiedChatCompletionResults;
import org.elasticsearch.xpack.inference.services.amazonbedrock.client.AmazonBedrockBaseClient;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Flow;

public class ToolAwareUnifiedPublisher implements Flow.Publisher<StreamingUnifiedChatCompletionResults.Results> {
    private final AmazonBedrockBaseClient client;
    private ConverseStreamRequest request;

    ToolAwareUnifiedPublisher(AmazonBedrockBaseClient client, ConverseStreamRequest request) {
        this.client = client;
        this.request = request;
    }

    @SuppressWarnings("checkstyle:DescendantToken")
    @Override
    public void subscribe(Flow.Subscriber<? super StreamingUnifiedChatCompletionResults.Results> subscriber) {
        subscriber.onSubscribe(new Flow.Subscription() {
            boolean cancelled = false;

            @SuppressWarnings("checkstyle:DescendantToken")
            @Override
            public void request(long n) {
                if (cancelled) {
                    return;
                }
                try {
                    var history = new ArrayList<>(request.messages());
                    ConverseStreamRequest currentRequest = request;

                    while (!cancelled) {
                        List<ToolUseInfo> toolUses = new ArrayList<>();

                        toolUses.add(new ToolUseInfo("tooluse_wYgv7Mx0Q_KsxRNbAoidLQ", "get_current_price"));

                        String[] stopReasons = new String[1];
                        var round = client.converseUnifiedStream(currentRequest.toBuilder().messages(history).build());

                        round.subscribe(new Flow.Subscriber<>() {
                            @Override
                            public void onSubscribe(Flow.Subscription subscription) {
                                subscription.request(Long.MAX_VALUE);
                            }

                            @Override
                            public void onNext(StreamingUnifiedChatCompletionResults.Results results) {
                                subscriber.onNext(results);

                                for (var chunk : results.chunks()) {
                                    for (var choice : chunk.choices()) {
                                        if (choice.finishReason() != null) {
                                            stopReasons[0] = choice.finishReason();
                                        }

                                        var delta = choice.delta();
                                        if (delta == null) {
                                            continue;
                                        }

                                        var calls = delta.toolCalls();
                                        if (calls != null && !calls.isEmpty()) {
                                            for (var call : calls) {
                                                String id = call.id();
                                                String name = call.function().name();

                                                if (id != null && name != null) {
                                                    toolUses.add(new ToolUseInfo(id, name));
                                                }

                                            }
                                        }
                                    }
                                }
                            }

                            @Override
                            public void onError(Throwable throwable) {
                                subscriber.onError(throwable);

                            }

                            @Override
                            public void onComplete() {
                                List<ContentBlock> toolResults = new ArrayList<>();
                                if (!toolUses.isEmpty()) {

                                    for (ToolUseInfo toolUse : toolUses) {
                                        String jsonIn = toolUse.inputJson.toString();
                                        // String jsonOut = execute(toolUse.getName(), jsonIn);

                                        toolResults.add(
                                            ContentBlock.builder()
                                                .toolResult(
                                                    ToolResultBlock.builder()
                                                        .toolUseId(toolUse.getId())
                                                        .content(ToolResultContentBlock.fromJson(Document.fromString(jsonIn)))
                                                        .build()
                                                )
                                                .build()
                                        );

                                    }
                                }

                                Message toolResultMsg = Message.builder().role(ConversationRole.USER).content(toolResults).build();

                                history.add(toolResultMsg);

                                client.converseUnifiedStream(
                                    ConverseStreamRequest.builder()
                                        .modelId(request.modelId())
                                        .messages(history)
                                        .toolConfig(
                                            ToolConfiguration.builder()
                                                .tools(
                                                    Tool.builder()
                                                        .toolSpec(
                                                            ToolSpecification.builder()
                                                                .name(toolUses.getFirst().getName())
                                                                .description(toolUses.getFirst().getId())
                                                                .inputSchema(
                                                                    ToolInputSchema.fromJson(
                                                                        Document.fromString(toolUses.getFirst().getInputJson().toString())
                                                                    )
                                                                )
                                                                .build()
                                                        )
                                                        .build()
                                                )
                                                .toolChoice(
                                                    ToolChoice.builder()
                                                        .tool(SpecificToolChoice.builder().name(toolUses.getFirst().getName()).build())
                                                        .build()
                                                )
                                                .build()
                                        )
                                        .build()
                                );

                            }
                        });

                        boolean toolRequested = "TOOL_USE".equalsIgnoreCase(stopReasons[0]) || !toolUses.isEmpty();

                        if (!toolRequested) {
                            break;
                        }
                    }
                    subscriber.onComplete();
                } catch (Throwable throwable) {
                    subscriber.onError(throwable);
                }
            }

            @Override
            public void cancel() {
                cancelled = true;
            }
        });
    }
}
