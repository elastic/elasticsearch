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
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsBuilder;
import org.elasticsearch.xpack.core.inference.chunking.SentenceBoundaryChunkingSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class ChunkScorerConfig implements Writeable, ToXContentObject {

    public final Integer size;
    private final String inferenceText;
    private final ChunkingSettings chunkingSettings;

    public static final int DEFAULT_CHUNK_SIZE = 300;
    public static final int DEFAULT_SIZE = 1;

    public static ChunkingSettings createChunkingSettings(Integer chunkSize) {
        int chunkSizeOrDefault = chunkSize != null ? chunkSize : DEFAULT_CHUNK_SIZE;
        ChunkingSettings chunkingSettings = new SentenceBoundaryChunkingSettings(chunkSizeOrDefault, 0);
        chunkingSettings.validate();
        return chunkingSettings;
    }

    public static ChunkingSettings chunkingSettingsFromMap(Map<String, Object> map) {

        if (map == null || map.isEmpty()) {
            return createChunkingSettings(DEFAULT_CHUNK_SIZE);
        }

        if (map.size() == 1 && map.containsKey("max_chunk_size")) {
            return createChunkingSettings((Integer) map.get("max_chunk_size"));
        }

        return ChunkingSettingsBuilder.fromMap(map);
    }

    public ChunkScorerConfig(StreamInput in) throws IOException {
        this.size = in.readOptionalVInt();
        this.inferenceText = in.readString();
        Map<String, Object> chunkingSettingsMap = in.readGenericMap();
        this.chunkingSettings = ChunkingSettingsBuilder.fromMap(chunkingSettingsMap);
    }

    public ChunkScorerConfig(Integer size, ChunkingSettings chunkingSettings) {
        this(size, null, chunkingSettings);
    }

    public ChunkScorerConfig(Integer size, String inferenceText, ChunkingSettings chunkingSettings) {
        this.size = size;
        this.inferenceText = inferenceText;
        this.chunkingSettings = chunkingSettings;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalVInt(size);
        out.writeString(inferenceText);
        out.writeGenericMap(chunkingSettings.asMap());
    }

    public Integer size() {
        return size;
    }

    public int sizeOrDefault() {
        return size != null ? size : DEFAULT_SIZE;
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
        ChunkScorerConfig that = (ChunkScorerConfig) o;
        return Objects.equals(size, that.size)
            && Objects.equals(inferenceText, that.inferenceText)
            && Objects.equals(chunkingSettings, that.chunkingSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(size, inferenceText, chunkingSettings);
    }

    @Override
    public String toString() {
        return "ChunkScorerConfig{"
            + "size="
            + sizeOrDefault()
            + ", inferenceText=["
            + inferenceText
            + ']'
            + ", chunkingSettings="
            + chunkingSettings
            + "}";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("size", size);
        builder.field("inference_text", inferenceText);
        builder.field("chunking_settings");
        chunkingSettings.toXContent(builder, params);
        builder.endObject();
        return builder;
    }
}
