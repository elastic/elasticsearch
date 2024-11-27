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

public class WordBoundaryChunkingSettings implements ChunkingSettings {
    public static final String NAME = "WordBoundaryChunkingSettings";
    private static final ChunkingStrategy STRATEGY = ChunkingStrategy.WORD;
    private static final int MAX_CHUNK_SIZE_LOWER_LIMIT = 10;
    private static final int MAX_CHUNK_SIZE_UPPER_LIMIT = 300;
    private static final Set<String> VALID_KEYS = Set.of(
        ChunkingSettingsOptions.STRATEGY.toString(),
        ChunkingSettingsOptions.MAX_CHUNK_SIZE.toString(),
        ChunkingSettingsOptions.OVERLAP.toString()
    );
    protected final int maxChunkSize;
    protected final int overlap;

    public WordBoundaryChunkingSettings(Integer maxChunkSize, Integer overlap) {
        this.maxChunkSize = maxChunkSize;
        this.overlap = overlap;
    }

    public WordBoundaryChunkingSettings(StreamInput in) throws IOException {
        maxChunkSize = in.readInt();
        overlap = in.readInt();
    }

    public static WordBoundaryChunkingSettings fromMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();

        var invalidSettings = map.keySet().stream().filter(key -> VALID_KEYS.contains(key) == false).toArray();
        if (invalidSettings.length > 0) {
            validationException.addValidationError(
                Strings.format("Word based chunking settings can not have the following settings: %s", Arrays.toString(invalidSettings))
            );
        }

        Integer maxChunkSize = ServiceUtils.extractRequiredPositiveIntegerBetween(
            map,
            ChunkingSettingsOptions.MAX_CHUNK_SIZE.toString(),
            MAX_CHUNK_SIZE_LOWER_LIMIT,
            MAX_CHUNK_SIZE_UPPER_LIMIT,
            ModelConfigurations.CHUNKING_SETTINGS,
            validationException
        );

        Integer overlap = null;
        if (maxChunkSize != null) {
            overlap = ServiceUtils.extractRequiredPositiveIntegerLessThanOrEqualToMax(
                map,
                ChunkingSettingsOptions.OVERLAP.toString(),
                maxChunkSize / 2,
                ModelConfigurations.CHUNKING_SETTINGS,
                validationException
            );
        }

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new WordBoundaryChunkingSettings(maxChunkSize, overlap);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(ChunkingSettingsOptions.STRATEGY.toString(), STRATEGY);
            builder.field(ChunkingSettingsOptions.MAX_CHUNK_SIZE.toString(), maxChunkSize);
            builder.field(ChunkingSettingsOptions.OVERLAP.toString(), overlap);
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
        out.writeInt(overlap);
    }

    @Override
    public ChunkingStrategy getChunkingStrategy() {
        return STRATEGY;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WordBoundaryChunkingSettings that = (WordBoundaryChunkingSettings) o;
        return Objects.equals(maxChunkSize, that.maxChunkSize) && Objects.equals(overlap, that.overlap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxChunkSize, overlap);
    }
}
