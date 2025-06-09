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
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class NoneChunkingSettings implements ChunkingSettings {
    public static final String NAME = "NoneChunkingSettings";
    public static NoneChunkingSettings INSTANCE = new NoneChunkingSettings();

    private static final ChunkingStrategy STRATEGY = ChunkingStrategy.NONE;
    private static final Set<String> VALID_KEYS = Set.of(ChunkingSettingsOptions.STRATEGY.toString());

    private NoneChunkingSettings() {}

    public NoneChunkingSettings(StreamInput in) throws IOException {}

    @Override
    public ChunkingStrategy getChunkingStrategy() {
        return STRATEGY;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        throw new IllegalStateException("not used");
    }

    @Override
    public boolean supportsVersion(TransportVersion version) {
        return version.isPatchFrom(TransportVersions.NONE_CHUNKING_STRATEGY_8_19)
            || version.onOrAfter(TransportVersions.NONE_CHUNKING_STRATEGY);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {}

    @Override
    public Map<String, Object> asMap() {
        return Map.of(ChunkingSettingsOptions.STRATEGY.toString(), STRATEGY.toString().toLowerCase(Locale.ROOT));
    }

    public static NoneChunkingSettings fromMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();

        var invalidSettings = map.keySet().stream().filter(key -> VALID_KEYS.contains(key) == false).toArray();
        if (invalidSettings.length > 0) {
            validationException.addValidationError(
                Strings.format(
                    "When chunking is disabled (none), settings can not have the following: %s",
                    Arrays.toString(invalidSettings)
                )
            );
        }

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new NoneChunkingSettings();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(ChunkingSettingsOptions.STRATEGY.toString(), STRATEGY);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass());
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
