/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.chunking;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.ChunkingStrategy;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ServiceUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class SentenceBoundaryChunkingSettings implements ChunkingSettings {
    public static final String NAME = "SentenceBoundaryChunkingSettings";
    private static final ChunkingStrategy STRATEGY = ChunkingStrategy.SENTENCE;
    private static final Set<String> VALID_KEYS = Set.of(
        ChunkingSettingsOptions.STRATEGY.toString(),
        ChunkingSettingsOptions.MAX_CHUNK_SIZE.toString()
    );
    protected final int maxChunkSize;

    public SentenceBoundaryChunkingSettings(Integer maxChunkSize) {
        this.maxChunkSize = maxChunkSize;
    }

    public SentenceBoundaryChunkingSettings(StreamInput in) throws IOException {
        maxChunkSize = in.readInt();
    }

    public static SentenceBoundaryChunkingSettings fromMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();

        var invalidSettings = map.keySet().stream().filter(key -> VALID_KEYS.contains(key) == false).toArray();
        if (invalidSettings.length > 0) {
            validationException.addValidationError(
                Strings.format("Sentence based chunking settings can not have the following settings: %s", Arrays.toString(invalidSettings))
            );
        }

        Integer maxChunkSize = ServiceUtils.extractRequiredPositiveInteger(
            map,
            ChunkingSettingsOptions.MAX_CHUNK_SIZE.toString(),
            ModelConfigurations.CHUNKING_SETTINGS,
            validationException
        );

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new SentenceBoundaryChunkingSettings(maxChunkSize);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(ChunkingSettingsOptions.STRATEGY.toString(), STRATEGY);
            builder.field(ChunkingSettingsOptions.MAX_CHUNK_SIZE.toString(), maxChunkSize);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ML_INFERENCE_CHUNKING_SETTINGS;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(maxChunkSize);
    }

    @Override
    public ChunkingStrategy getChunkingStrategy() {
        return STRATEGY;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SentenceBoundaryChunkingSettings that = (SentenceBoundaryChunkingSettings) o;
        return Objects.equals(maxChunkSize, that.maxChunkSize);
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxChunkSize);
    }
}
