/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.request.completion;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.completion.Message;
import org.elasticsearch.inference.completion.ReasoningDetail.TextReasoningDetail;
import org.elasticsearch.inference.completion.ToolCall;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiUnifiedStreamingProcessor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Decides which thought signature each part of a Gemini {@code Content} carries when unified chat completion messages
 * are replayed to Gemini.
 * <p>
 * One instance covers one Gemini content. Consecutive messages with the same role are merged into a single content, and
 * a merged {@code model} content is one <em>step</em> in Google's terms. Gemini 3 only validates the first
 * {@code functionCall} part of each step, so the {@link #SKIP_THOUGHT_SIGNATURE_VALIDATOR} fallback must be written at
 * most once per content, not once per message. That is why the state lives here and not in the code that writes a
 * single message.
 * <p>
 * See <a href="https://ai.google.dev/gemini-api/docs/generate-content/thought-signatures#how_it_works">how signatures
 * work</a> and
 * <a href="https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/thinking/thought-signatures#using-rest-or-manual-handling">
 * handling them manually</a>.
 */
final class GoogleVertexAiThoughtSignatures {
    private static final Logger logger = LogManager.getLogger(GoogleVertexAiThoughtSignatures.class);

    /**
     * Google's documented placeholder that tells Gemini to skip thought signature validation. Used when a client
     * replays a function call without the signature Gemini issued for it (e.g. because the client does not yet support
     * {@code reasoning_details}), which Gemini 3 would otherwise reject with a 400.
     * <p>
     * The equivalent sentinel {@code context_engineering_is_the_way_to_go} is also accepted. Both are documented by
     * Google for this use case; this one is used by gemini-cli, LiteLLM, and pydantic-ai.
     * See <a href="https://ai.google.dev/gemini-api/docs/generate-content/thought-signatures">thought signatures</a>.
     */
    static final String SKIP_THOUGHT_SIGNATURE_VALIDATOR = "skip_thought_signature_validator";

    /**
     * {@code true} once a function call has been written to this content, whichever message it came from.
     */
    private boolean functionCallWritten = false;

    /**
     * Resolves the signatures carried by one message of this content. The messages of a content must be passed in
     * order, and each message's function calls must be resolved in order with {@link MessageSignatures#forFunctionCall}.
     */
    MessageSignatures forMessage(Message message) {
        return new MessageSignatures(textReasoningDetails(message));
    }

    /**
     * The signatures of a single message, resolved against the function calls already written to the content.
     */
    final class MessageSignatures {
        private final List<TextReasoningDetail> googleDetails;
        private final Map<String, String> signaturesByToolCallId;
        @Nullable
        private String unboundSignature;

        private MessageSignatures(List<TextReasoningDetail> googleDetails) {
            this.googleDetails = googleDetails;
            this.signaturesByToolCallId = signaturesByToolCallId(googleDetails);
            this.unboundSignature = unboundSignature(googleDetails);
        }

        /**
         * The thought summaries to write ahead of the message's content, i.e. the details that carry thought text.
         */
        List<TextReasoningDetail> thoughtSummaries() {
            return googleDetails.stream().filter(detail -> detail.text() != null).toList();
        }

        /**
         * A signature with no {@code id} and no text belongs to the text of the message, so it goes on the last text
         * part. Returns it once; afterwards, or when there was none, returns {@code null}. When the message has no
         * text, the signature is left for {@link #forFunctionCall}.
         */
        @Nullable
        String takeForLastTextPart() {
            var signature = unboundSignature;
            unboundSignature = null;
            return signature;
        }

        /**
         * Returns the signature to write on a function call, or {@code null} when it carries none.
         * <p>
         * A signature bound to the call's id always wins. Otherwise an unbound signature that no text part used is
         * attached to the first call that has none of its own. If there is still no signature and no function call has
         * been written to this content yet, {@link #SKIP_THOUGHT_SIGNATURE_VALIDATOR} is returned so that Gemini 3 does
         * not reject the request with a 400. Real signatures always take precedence; the sentinel is only a fallback.
         */
        @Nullable
        String forFunctionCall(ToolCall toolCall) {
            var signature = signaturesByToolCallId.get(toolCall.id());
            if (signature == null && unboundSignature != null) {
                // Google attaches the signature to the first function call of a step, so an unbound signature
                // belongs to the first call that does not already carry one.
                signature = unboundSignature;
                unboundSignature = null;
            }
            if (signature == null && functionCallWritten == false) {
                // Gemini 3 requires a thought signature on the first functionCall of a step. When the client has
                // not sent reasoning_details (e.g. because the client predates that field), use Google's sentinel
                // so the request is not rejected with a 400.
                logger.debug(
                    "No thought signature for first function call [{}]; using skip-validator sentinel",
                    toolCall.function().name()
                );
                signature = SKIP_THOUGHT_SIGNATURE_VALIDATOR;
            }
            functionCallWritten = true;
            return signature;
        }
    }

    /**
     * Returns the {@link TextReasoningDetail} entries from the message that were produced by this provider
     * ({@code format == google-vertex-ai-v1}). Details from other providers (e.g. Anthropic) are filtered out to avoid
     * sending foreign signatures to Gemini, which would result in a 400.
     */
    private static List<TextReasoningDetail> textReasoningDetails(Message message) {
        if (message.reasoningDetails() == null) {
            return List.of();
        }
        return message.reasoningDetails()
            .stream()
            .filter(TextReasoningDetail.class::isInstance)
            .map(TextReasoningDetail.class::cast)
            .filter(detail -> GoogleVertexAiUnifiedStreamingProcessor.GOOGLE_VERTEX_AI_FORMAT.equals(detail.format()))
            .toList();
    }

    private static Map<String, String> signaturesByToolCallId(List<TextReasoningDetail> details) {
        var signatures = new HashMap<String, String>();
        for (var reasoningDetail : details) {
            if (reasoningDetail.id() != null && reasoningDetail.signature() != null) {
                signatures.put(reasoningDetail.id(), reasoningDetail.signature());
            }
        }
        return signatures;
    }

    /**
     * The signature of a reasoning detail that names neither a tool call nor any thought text, and so has to be
     * matched to a part positionally.
     * <p>
     * Google documents a single place for such a signature in a response: the last part, which is an empty text part
     * when streaming. Signatures on function calls are always bound to the call's id. So a message only carries more
     * than one unbound signature when a client has merged several responses into one message. Gemini does not strictly
     * validate the signatures of non-function-call parts, so the first is used and the rest are dropped.
     * <p>
     * See <a href="https://ai.google.dev/gemini-api/docs/generate-content/thought-signatures#how_it_works">how
     * signatures work</a> and
     * <a href="https://ai.google.dev/gemini-api/docs/generate-content/thought-signatures#non-function-call">signatures
     * in non-function-call parts</a>.
     */
    @Nullable
    private static String unboundSignature(List<TextReasoningDetail> details) {
        var unboundSignatures = details.stream()
            .filter(detail -> detail.id() == null && detail.text() == null && detail.signature() != null)
            .map(TextReasoningDetail::signature)
            .toList();
        if (unboundSignatures.isEmpty()) {
            return null;
        }
        if (unboundSignatures.size() > 1) {
            logger.debug("Message has [{}] unbound thought signatures; using the first and dropping the rest", unboundSignatures.size());
        }
        return unboundSignatures.getFirst();
    }
}
