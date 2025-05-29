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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.ChunkingStrategy;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ServiceUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class SentenceBoundaryChunkingSettings implements ChunkingSettings {
    public static final String NAME = "SentenceBoundaryChunkingSettings";
    private static final ChunkingStrategy STRATEGY = ChunkingStrategy.SENTENCE;
    private static final int MAX_CHUNK_SIZE_LOWER_LIMIT = 20;
    private static final int MAX_CHUNK_SIZE_UPPER_LIMIT = 300;
    private static final Set<String> VALID_KEYS = Set.of(
        ChunkingSettingsOptions.STRATEGY.toString(),
        ChunkingSettingsOptions.MAX_CHUNK_SIZE.toString(),
        ChunkingSettingsOptions.SENTENCE_OVERLAP.toString()
    );

    private static int DEFAULT_OVERLAP = 1;

    protected final int maxChunkSize;
    protected int sentenceOverlap = DEFAULT_OVERLAP;

    public SentenceBoundaryChunkingSettings(Integer maxChunkSize, @Nullable Integer sentenceOverlap) {
        this.maxChunkSize = maxChunkSize;
        this.sentenceOverlap = sentenceOverlap == null ? DEFAULT_OVERLAP : sentenceOverlap;
    }

    public SentenceBoundaryChunkingSettings(StreamInput in) throws IOException {
        maxChunkSize = in.readInt();
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
            sentenceOverlap = in.readVInt();
        }
    }

    @Override
    public Map<String, Object> asMap() {
        return Map.of(
            ChunkingSettingsOptions.STRATEGY.toString(),
            STRATEGY.toString().toLowerCase(Locale.ROOT),
            ChunkingSettingsOptions.MAX_CHUNK_SIZE.toString(),
            maxChunkSize,
            ChunkingSettingsOptions.SENTENCE_OVERLAP.toString(),
            sentenceOverlap
        );
    }

    public static SentenceBoundaryChunkingSettings fromMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();

        var invalidSettings = map.keySet().stream().filter(key -> VALID_KEYS.contains(key) == false).toArray();
        if (invalidSettings.length > 0) {
            validationException.addValidationError(
                Strings.format("Sentence based chunking settings can not have the following settings: %s", Arrays.toString(invalidSettings))
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

        Integer sentenceOverlap = ServiceUtils.removeAsType(
            map,
            ChunkingSettingsOptions.SENTENCE_OVERLAP.toString(),
            Integer.class,
            validationException
        );
        if (sentenceOverlap == null) {
            sentenceOverlap = DEFAULT_OVERLAP;
        } else if (sentenceOverlap > 1 || sentenceOverlap < 0) {
            validationException.addValidationError(
                ChunkingSettingsOptions.SENTENCE_OVERLAP + "[" + sentenceOverlap + "] must be either 0 or 1"
            );
        }

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new SentenceBoundaryChunkingSettings(maxChunkSize, sentenceOverlap);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(ChunkingSettingsOptions.STRATEGY.toString(), STRATEGY);
            builder.field(ChunkingSettingsOptions.MAX_CHUNK_SIZE.toString(), maxChunkSize);
            builder.field(ChunkingSettingsOptions.SENTENCE_OVERLAP.toString(), sentenceOverlap);
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
        return TransportVersions.V_8_16_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(maxChunkSize);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
            out.writeVInt(sentenceOverlap);
        }
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
        return Objects.equals(maxChunkSize, that.maxChunkSize) && Objects.equals(sentenceOverlap, that.sentenceOverlap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxChunkSize, sentenceOverlap);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
