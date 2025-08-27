/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rank.textsimilarity;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.xpack.inference.chunking.ChunkingSettingsBuilder;
import org.elasticsearch.xpack.inference.chunking.SentenceBoundaryChunkingSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class SnippetConfig implements Writeable {

    public final Integer numSnippets;
    private final String inferenceText;
    private final ChunkingSettings chunkingSettings;

    public static final int DEFAULT_CHUNK_SIZE = 300;
    public static final int DEFAULT_NUM_SNIPPETS = 1;

    public static ChunkingSettings createChunkingSettings(Integer chunkSize) {
        int chunkSizeOrDefault = chunkSize != null ? chunkSize : DEFAULT_CHUNK_SIZE;
        ChunkingSettings chunkingSettings = new SentenceBoundaryChunkingSettings(chunkSizeOrDefault, 0);
        chunkingSettings.validate();
        return chunkingSettings;
    }

    public SnippetConfig(StreamInput in) throws IOException {
        this.numSnippets = in.readOptionalVInt();
        this.inferenceText = in.readString();
        Map<String, Object> chunkingSettingsMap = in.readGenericMap();
        this.chunkingSettings = ChunkingSettingsBuilder.fromMap(chunkingSettingsMap);
    }

    public SnippetConfig(Integer numSnippets, ChunkingSettings chunkingSettings, Integer chunkSize) {
        this(numSnippets, null, chunkingSettings, chunkSize);
    }

    public SnippetConfig(Integer numSnippets, String inferenceText, Integer chunkSize) {
        this(numSnippets, inferenceText, null, chunkSize);
    }

    public SnippetConfig(Integer numSnippets, String inferenceText, ChunkingSettings chunkingSettings) {
        this(numSnippets, inferenceText, chunkingSettings, null);
    }

    public SnippetConfig(Integer numSnippets, String inferenceText, ChunkingSettings chunkingSettings, Integer chunkSize) {

        if (chunkingSettings != null && chunkSize != null) {
            throw new IllegalArgumentException("Only one of chunking_settings or chunk_size may be provided");
        }

        this.numSnippets = numSnippets;
        this.inferenceText = inferenceText;
        this.chunkingSettings = chunkingSettings != null ? chunkingSettings : createChunkingSettings(chunkSize);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalVInt(numSnippets);
        out.writeString(inferenceText);
        out.writeGenericMap(chunkingSettings.asMap());
    }

    public Integer numSnippets() {
        return numSnippets;
    }

    public String inferenceText() {
        return inferenceText;
    }

    public ChunkingSettings chunkingSettings() {
        return chunkingSettings;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SnippetConfig that = (SnippetConfig) o;
        return Objects.equals(numSnippets, that.numSnippets)
            && Objects.equals(inferenceText, that.inferenceText)
            && Objects.equals(chunkingSettings, that.chunkingSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(numSnippets, inferenceText, chunkingSettings);
    }
}
