/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.chunking;

import org.elasticsearch.TransportVersion;
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
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class RecursiveChunkingSettings implements ChunkingSettings {
    public static final String NAME = "RecursiveChunkingSettings";
    private static final ChunkingStrategy STRATEGY = ChunkingStrategy.RECURSIVE;
    private static final int MAX_CHUNK_SIZE_LOWER_LIMIT = 10;
    private static final int MAX_CHUNK_SIZE_UPPER_LIMIT = 300;

    private static final Set<String> VALID_KEYS = Set.of(
        ChunkingSettingsOptions.STRATEGY.toString(),
        ChunkingSettingsOptions.MAX_CHUNK_SIZE.toString(),
        ChunkingSettingsOptions.SEPARATOR_SET.toString(),
        ChunkingSettingsOptions.SEPARATORS.toString()
    );

    protected static final Map<SeparatorSet, List<String>> SEPARATOR_SETS = Map.of(
        SeparatorSet.DEFAULT,
        List.of("(?<!\n)\n\n(?!\n)", "(?<!\n)\n(?!\n)"),
        SeparatorSet.MARKDOWN,
        List.of("(?<!#)###(?!#)", "(?<!#)##(?!#)", "(?<!#)#(?!#)") // TODO: What other ones do we want here?
    );

    private final int maxChunkSize;
    private final List<String> separators;

    public RecursiveChunkingSettings(int maxChunkSize, List<String> separators) {
        this.maxChunkSize = maxChunkSize;
        this.separators = separators == null ? SEPARATOR_SETS.get(SeparatorSet.DEFAULT) : separators;
    }

    public RecursiveChunkingSettings(StreamInput in) throws IOException {
        maxChunkSize = in.readInt();
        separators = in.readCollectionAsList(StreamInput::readString);
    }

    public static RecursiveChunkingSettings fromMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();

        var invalidSettings = map.keySet().stream().filter(key -> VALID_KEYS.contains(key) == false).toArray();
        if (invalidSettings.length > 0) {
            validationException.addValidationError(
                Strings.format("Recursive chunking settings can not have the following settings: %s", Arrays.toString(invalidSettings))
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

        SeparatorSet separatorSet = ServiceUtils.extractOptionalEnum(
            map,
            ChunkingSettingsOptions.SEPARATOR_SET.toString(),
            ModelConfigurations.CHUNKING_SETTINGS,
            SeparatorSet::fromString,
            EnumSet.allOf(SeparatorSet.class),
            validationException
        );

        List<String> separators = ServiceUtils.extractOptionalList(
            map,
            ChunkingSettingsOptions.SEPARATORS.toString(),
            String.class,
            validationException
        );

        if (separators != null && separatorSet != null) {
            validationException.addValidationError("Recursive chunking settings can not have both separators and separator_set");
        }

        if (separatorSet != null) {
            separators = SEPARATOR_SETS.get(separatorSet);
        }

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new RecursiveChunkingSettings(maxChunkSize, separators);
    }

    public int getMaxChunkSize() {
        return maxChunkSize;
    }

    public List<String> getSeparators() {
        return separators;
    }

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
        return null; // TODO: Add transport version
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(maxChunkSize);
        out.writeCollection(separators, StreamOutput::writeString);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(ChunkingSettingsOptions.STRATEGY.toString(), STRATEGY);
            builder.field(ChunkingSettingsOptions.MAX_CHUNK_SIZE.toString(), maxChunkSize);
            builder.field(ChunkingSettingsOptions.SEPARATORS.toString(), separators);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RecursiveChunkingSettings that = (RecursiveChunkingSettings) o;
        return Objects.equals(maxChunkSize, that.maxChunkSize) && Objects.equals(separators, that.separators);
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxChunkSize, separators);
    }

    protected enum SeparatorSet {
        DEFAULT("default"),
        MARKDOWN("markdown");

        private final String name;

        SeparatorSet(String name) {
            this.name = name;
        }

        public static SeparatorSet fromString(String name) {
            return EnumSet.allOf(SeparatorSet.class)
                .stream()
                .filter(ss -> ss.name.equals(name))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(Strings.format("Invalid separator set %s", name)));
        }
    }
}
