/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai.completion;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiServiceSettings;

import java.io.IOException;
import java.util.Map;

/**
 * Service settings of the OCI Generative AI {@code completion} and {@code chat_completion} tasks. Chat completion adds no settings
 * of its own to the common OCI Generative AI settings.
 */
public class OciGenAiChatCompletionServiceSettings extends OciGenAiServiceSettings {

    public static final String NAME = "oci_genai_chat_completion_service_settings";

    private static final ObjectParser<Builder, ConfigurationParseContext> REQUEST_PARSER = createParser(false);
    private static final ObjectParser<Builder, ConfigurationParseContext> PERSISTENT_PARSER = createParser(true);

    /**
     * @param ignoreUnknownFields whether the parser should tolerate unknown fields. This is {@code false} for request parsing (so that
     *                            unexpected fields are rejected) and {@code true} for persisted configuration (so that fields written by
     *                            other versions are tolerated).
     */
    static ObjectParser<Builder, ConfigurationParseContext> createParser(boolean ignoreUnknownFields) {
        return OciGenAiServiceSettings.buildCommonParser(ignoreUnknownFields, Builder::new);
    }

    public static OciGenAiChatCompletionServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        var parser = context == ConfigurationParseContext.REQUEST ? REQUEST_PARSER : PERSISTENT_PARSER;
        return OciGenAiServiceSettings.fromMap(map, context, parser);
    }

    public OciGenAiChatCompletionServiceSettings(CommonSettings common) {
        super(common);
    }

    public OciGenAiChatCompletionServiceSettings(StreamInput in) throws IOException {
        this(new CommonSettings(in));
    }

    @Override
    public OciGenAiChatCompletionServiceSettings updateServiceSettings(Map<String, Object> serviceSettings) {
        return parseUpdate(serviceSettings, Update.PARSER).mergeInto(this);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        common().writeTo(out);
    }

    /**
     * Builds an {@link OciGenAiChatCompletionServiceSettings} from the common OCI Generative AI fields.
     */
    public static class Builder extends OciGenAiServiceSettings.Builder<OciGenAiChatCompletionServiceSettings> {

        @Override
        protected OciGenAiChatCompletionServiceSettings build(CommonSettings common) {
            return new OciGenAiChatCompletionServiceSettings(common);
        }
    }

    /**
     * Parses an update request, which may only contain the mutable {@code rate_limit} field (and the signing key fields). Including
     * any immutable field (such as {@code model_id} or {@code region}) causes the strict parser to reject the request.
     */
    private static class Update extends OciGenAiServiceSettings.CommonUpdate {

        private static final ObjectParser<Update, Void> PARSER = OciGenAiServiceSettings.buildCommonUpdateParser(Update::new);

        OciGenAiChatCompletionServiceSettings mergeInto(OciGenAiChatCompletionServiceSettings existing) {
            return new OciGenAiChatCompletionServiceSettings(existing.common().withRateLimitSettings(mergedRateLimitSettings(existing)));
        }
    }
}
