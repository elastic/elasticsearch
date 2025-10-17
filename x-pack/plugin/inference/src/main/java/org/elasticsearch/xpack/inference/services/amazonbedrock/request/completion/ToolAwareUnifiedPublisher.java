/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.request.completion;

import org.elasticsearch.xpack.core.inference.results.StreamingUnifiedChatCompletionResults;
import org.elasticsearch.xpack.inference.services.amazonbedrock.client.AmazonBedrockBaseClient;

import software.amazon.awssdk.services.bedrockruntime.model.ContentBlock;
import software.amazon.awssdk.services.bedrockruntime.model.ConversationRole;
import software.amazon.awssdk.services.bedrockruntime.model.ConverseStreamRequest;
import software.amazon.awssdk.services.bedrockruntime.model.Message;
import software.amazon.awssdk.services.bedrockruntime.model.ToolResultBlock;

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

                            }
                        });


                        boolean toolRequested = "TOOL_USE".equalsIgnoreCase(stopReasons[0]) || !toolUses.isEmpty();

                        if (!toolRequested) {
                            break;
                        }

                        List<ContentBlock> toolResultBlocks = new ArrayList<>();
                        for (var toolUse : toolUses) {

                            String jsonIn = toolUse.inputJson.toString();
//                          String jsonOut = execute(toolUse.getName(), jsonIn);

                            toolResultBlocks.add(
                                ContentBlock.builder()
                                    .toolResult(ToolResultBlock.builder()
                                        .toolUseId(toolUse.getId())
//                                      .content((Collection<ToolResultContentBlock>) Document.fromString(jsonOut))
                                        .build())
                                    .build());

                            Message toolResultMsg = Message.builder()
                                .role(ConversationRole.USER)
                                .content(toolResultBlocks)
                                .build();

                            history.add(toolResultMsg);

                            currentRequest = currentRequest.toBuilder().messages(history).build();
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
