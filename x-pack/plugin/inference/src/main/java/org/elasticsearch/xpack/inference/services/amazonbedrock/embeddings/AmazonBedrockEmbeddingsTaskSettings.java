/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.common.model.Truncation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalEnum;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.TRUNCATE_FIELD;

public record AmazonBedrockEmbeddingsTaskSettings(@Nullable Truncation truncation) implements TaskSettings {
    public static final AmazonBedrockEmbeddingsTaskSettings EMPTY = new AmazonBedrockEmbeddingsTaskSettings((Truncation) null);
    public static final String NAME = "amazon_bedrock_embeddings_task_settings";
    private static final TransportVersion AMAZON_BEDROCK_TASK_SETTINGS = TransportVersion.fromName("amazon_bedrock_task_settings");

    public static AmazonBedrockEmbeddingsTaskSettings fromMap(Map<String, Object> map) {
        if (map == null || map.isEmpty()) {
            return EMPTY;
        }

        ValidationException validationException = new ValidationException();

        var extractedTruncation = extractOptionalEnum(
            map,
            TRUNCATE_FIELD,
            ModelConfigurations.TASK_SETTINGS,
            Truncation::fromString,
            Truncation.ALL,
            validationException
        );

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new AmazonBedrockEmbeddingsTaskSettings(extractedTruncation);
    }

    public AmazonBedrockEmbeddingsTaskSettings(StreamInput in) throws IOException {
        this(in.readOptionalEnum(Truncation.class));
    }

    @Override
    public boolean isEmpty() {
        return truncation() == null;
    }

    @Override
    public AmazonBedrockEmbeddingsTaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        var newTaskSettings = fromMap(new HashMap<>(newSettings));

        return new AmazonBedrockEmbeddingsTaskSettings(firstNonNullOrNull(newTaskSettings.truncation(), truncation()));
    }

    private static <T> T firstNonNullOrNull(T first, T second) {
        return first != null ? first : second;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        assert false : "should never be called when supportsVersion is used";
        return AMAZON_BEDROCK_TASK_SETTINGS;
    }

    @Override
    public boolean supportsVersion(TransportVersion version) {
        return version.supports(AMAZON_BEDROCK_TASK_SETTINGS);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalEnum(truncation());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (truncation != null) {
            builder.field(TRUNCATE_FIELD, truncation);
        }
        return builder.endObject();
    }
}
