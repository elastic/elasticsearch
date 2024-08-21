/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.chunking;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ServiceUtils;

import java.io.IOException;
import java.util.Map;

public class WordBoundaryChunkingSettings extends ChunkingSettings {
    public static final String NAME = "WordBoundaryChunkingSettings";
    protected static final String STRATEGY = ChunkingStrategy.WORD.toString();
    protected final int maxChunkSize;
    protected final int overlap;

    public WordBoundaryChunkingSettings(Integer maxChunkSize, Integer overlap) {
        super(STRATEGY);
        this.maxChunkSize = maxChunkSize;
        this.overlap = overlap;
    }

    public WordBoundaryChunkingSettings(StreamInput in) throws IOException {
        super(STRATEGY);
        maxChunkSize = in.readInt();
        overlap = in.readInt();
    }

    public static WordBoundaryChunkingSettings fromMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();
        Integer maxChunkSize = ServiceUtils.extractRequiredPositiveInteger(
            map,
            ChunkingSettingsOptions.MAX_CHUNK_SIZE.toString(),
            ModelConfigurations.CHUNKING_SETTINGS,
            validationException
        );
        Integer overlap = ServiceUtils.extractRequiredPositiveIntegerLessThanOrEqualToMax(
            map,
            ChunkingSettingsOptions.OVERLAP.toString(),
            maxChunkSize != null ? maxChunkSize / 2 : null,
            ModelConfigurations.CHUNKING_SETTINGS,
            validationException
        );

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
}
