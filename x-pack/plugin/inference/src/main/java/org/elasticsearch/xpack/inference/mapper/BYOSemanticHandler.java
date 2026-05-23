/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.InferenceMetadataFieldsMapper;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class for detecting and dispatching BYO (bring-your-own) semantic_text
 * field values. A BYO value is one where the caller supplies pre-computed chunks
 * and vectors rather than asking Elasticsearch to run inference.
 *
 * <p>Two input shapes are recognised:
 * <ul>
 *   <li><em>Multi-part staging</em> — a Map containing an {@code _action} key
 *       (and optionally {@code text}). Each request corresponds to one phase of
 *       a multi-part upload session.</li>
 *   <li><em>Single-shot BYO</em> — a Map containing both {@code text} and
 *       {@code chunks} keys. The full embedding payload is delivered in one
 *       document.</li>
 * </ul>
 *
 * <p>Ordinary inference result Maps (e.g. {@code {"inference": {...}}}) and plain
 * String values are <em>not</em> considered BYO values.
 */
public final class BYOSemanticHandler {

    static final String STAGED_KEY = "_staged";

    private BYOSemanticHandler() {}

    /**
     * Returns {@code true} if {@code fieldValue} is a BYO semantic_text input.
     *
     * <p>A value is BYO when it is a {@link Map} that contains either:
     * <ul>
     *   <li>an {@code _action} key (multi-part staging protocol), or</li>
     *   <li>both {@code text} and {@code chunks} keys (single-shot BYO).</li>
     * </ul>
     *
     * @param fieldValue the raw field value from the document; may be {@code null}
     * @return {@code true} for BYO values, {@code false} for strings, nulls, and
     *         non-BYO maps such as pre-computed inference result maps
     */
    public static boolean isBYOValue(@Nullable Object fieldValue) {
        if (fieldValue instanceof Map<?, ?> map) {
            if (map.containsKey(BYOSemanticAction.actionField())) {
                return true;
            }
            return map.containsKey("text") && map.containsKey("chunks");
        }
        return false;
    }

    /**
     * Extracts the {@link BYOSemanticAction} from a BYO field value Map.
     *
     * <p>Returns {@code null} for single-shot BYO values (those that carry
     * {@code text} and {@code chunks} but no {@code _action} key).
     *
     * @param fieldValue a Map that has already been confirmed as a BYO value via
     *                   {@link #isBYOValue(Object)}
     * @return the parsed action, or {@code null} for single-shot BYO
     * @throws IllegalArgumentException if the {@code _action} value is present but
     *                                  unrecognised
     */
    @Nullable
    public static BYOSemanticAction getAction(Map<String, Object> fieldValue) {
        Object actionValue = fieldValue.get(BYOSemanticAction.actionField());
        if (actionValue == null) {
            return null;
        }
        return BYOSemanticAction.fromString(actionValue.toString());
    }

    /**
     * Handles the {@code stage_init} action: creates initial staged data for a field.
     *
     * @param fieldName   the semantic_text field name
     * @param fieldValue  the BYO field value map containing text and optional chunks
     * @param docSourceMap the full document source map (will be modified)
     * @param now         timestamp for lastModified
     */
    public static void handleStageInit(String fieldName, Map<String, Object> fieldValue, Map<String, Object> docSourceMap, Instant now) {
        Map<String, Object> existingStaged = getStaged(fieldName, docSourceMap);
        if (existingStaged != null) {
            throw new ElasticsearchStatusException(
                "Field [{}] already has staged data; commit or cancel before starting a new session",
                RestStatus.BAD_REQUEST,
                fieldName
            );
        }

        String text = extractText(fieldName, fieldValue);
        int textLength = text.length();

        List<SemanticTextField.Chunk> chunks = parseChunksFromValue(fieldValue, textLength);
        if (chunks.isEmpty() == false) {
            for (SemanticTextField.Chunk chunk : chunks) {
                BYOSemanticValidator.validateChunk(chunk.startOffset(), chunk.endOffset(), chunk.rawEmbeddings(), textLength);
            }
            BYOSemanticValidator.validateNoOverlaps(Collections.emptyList(), chunks);
        }

        StagedSemanticField staged = new StagedSemanticField(text, textLength, now, chunks);
        setStaged(fieldName, staged, docSourceMap);
    }

    /**
     * Handles the {@code stage} action: appends chunks to existing staged data.
     *
     * @param fieldName   the semantic_text field name
     * @param fieldValue  the BYO field value map containing chunks
     * @param docSourceMap the full document source map (will be modified)
     * @param now         timestamp for lastModified
     */
    public static void handleStage(String fieldName, Map<String, Object> fieldValue, Map<String, Object> docSourceMap, Instant now) {
        StagedSemanticField staged = getStagedOrThrow(fieldName, docSourceMap);
        int textLength = staged.textLength();

        List<SemanticTextField.Chunk> incomingChunks = parseChunksFromValue(fieldValue, textLength);
        if (incomingChunks.isEmpty()) {
            throw new ElasticsearchStatusException("No chunks provided in stage request for field [{}]", RestStatus.BAD_REQUEST, fieldName);
        }

        for (SemanticTextField.Chunk chunk : incomingChunks) {
            BYOSemanticValidator.validateChunk(chunk.startOffset(), chunk.endOffset(), chunk.rawEmbeddings(), textLength);
        }
        BYOSemanticValidator.validateNoOverlaps(staged.chunks(), incomingChunks);

        StagedSemanticField updated = staged.withAppendedChunks(incomingChunks, now);
        setStaged(fieldName, updated, docSourceMap);
    }

    /**
     * Handles the {@code commit} action: validates full coverage and promotes
     * staged data to committed inference metadata.
     *
     * @param fieldName     the semantic_text field name
     * @param fieldValue    the BYO field value map, may contain final chunks
     * @param docSourceMap  the full document source map (will be modified)
     * @param inferenceId   the inference endpoint ID for this field
     * @param modelSettings the model settings for dimensional validation
     * @param now           timestamp for lastModified
     */
    public static void handleCommit(
        String fieldName,
        Map<String, Object> fieldValue,
        Map<String, Object> docSourceMap,
        String inferenceId,
        MinimalServiceSettings modelSettings,
        Instant now
    ) {
        StagedSemanticField staged = getStagedOrThrow(fieldName, docSourceMap);
        int textLength = staged.textLength();

        List<SemanticTextField.Chunk> finalChunks = parseChunksFromValue(fieldValue, textLength);
        if (finalChunks.isEmpty() == false) {
            for (SemanticTextField.Chunk chunk : finalChunks) {
                BYOSemanticValidator.validateChunk(chunk.startOffset(), chunk.endOffset(), chunk.rawEmbeddings(), textLength);
            }
            BYOSemanticValidator.validateNoOverlaps(staged.chunks(), finalChunks);
            staged = staged.withAppendedChunks(finalChunks, now);
        }

        List<SemanticTextField.Chunk> allChunks = staged.chunks();
        BYOSemanticValidator.validateFullCoverage(allChunks, textLength);
        validateDimensions(allChunks, modelSettings, fieldName);

        promoteToCommitted(fieldName, staged, inferenceId, modelSettings, docSourceMap);
    }

    /**
     * Handles the {@code cancel} action: removes staged data.
     *
     * @param fieldName   the semantic_text field name
     * @param docSourceMap the full document source map (will be modified)
     */
    public static void handleCancel(String fieldName, Map<String, Object> docSourceMap) {
        getStagedOrThrow(fieldName, docSourceMap);
        removeStaged(fieldName, docSourceMap);
    }

    /**
     * Handles single-shot BYO: validates and writes committed inference metadata
     * in a single request, with no staging.
     *
     * @param fieldName     the semantic_text field name
     * @param fieldValue    the BYO field value map containing text and chunks
     * @param docSourceMap  the full document source map (will be modified)
     * @param inferenceId   the inference endpoint ID for this field
     * @param modelSettings the model settings for dimensional validation
     */
    public static void handleSingleShot(
        String fieldName,
        Map<String, Object> fieldValue,
        Map<String, Object> docSourceMap,
        String inferenceId,
        MinimalServiceSettings modelSettings
    ) {
        String text = extractText(fieldName, fieldValue);
        int textLength = text.length();

        List<SemanticTextField.Chunk> chunks = parseChunksFromValue(fieldValue, textLength);
        if (chunks.isEmpty()) {
            throw new ElasticsearchStatusException(
                "No chunks provided for single-shot BYO on field [{}]",
                RestStatus.BAD_REQUEST,
                fieldName
            );
        }

        for (SemanticTextField.Chunk chunk : chunks) {
            BYOSemanticValidator.validateChunk(chunk.startOffset(), chunk.endOffset(), chunk.rawEmbeddings(), textLength);
        }
        BYOSemanticValidator.validateNoOverlaps(Collections.emptyList(), chunks);
        BYOSemanticValidator.validateFullCoverage(chunks, textLength);
        validateDimensions(chunks, modelSettings, fieldName);

        StagedSemanticField staged = new StagedSemanticField(text, textLength, Instant.now(), chunks);
        promoteToCommitted(fieldName, staged, inferenceId, modelSettings, docSourceMap);
    }

    // ---- Helper methods ----

    private static String extractText(String fieldName, Map<String, Object> fieldValue) {
        Object textObj = fieldValue.get("text");
        if (textObj == null || (textObj instanceof String s && s.isEmpty())) {
            throw new ElasticsearchStatusException(
                "Field [{}] requires a non-empty 'text' value for BYO semantic_text",
                RestStatus.BAD_REQUEST,
                fieldName
            );
        }
        return textObj.toString();
    }

    @SuppressWarnings("unchecked")
    static List<SemanticTextField.Chunk> parseChunksFromValue(Map<String, Object> fieldValue, int textLength) {
        Object chunksObj = fieldValue.get("chunks");
        if (chunksObj == null) {
            return Collections.emptyList();
        }
        if (chunksObj instanceof List<?> == false) {
            throw new ElasticsearchStatusException("'chunks' must be an array", RestStatus.BAD_REQUEST);
        }
        List<?> chunksList = (List<?>) chunksObj;
        List<SemanticTextField.Chunk> result = new ArrayList<>(chunksList.size());
        for (Object chunkObj : chunksList) {
            if (chunkObj instanceof Map<?, ?> == false) {
                throw new ElasticsearchStatusException("Each chunk must be an object", RestStatus.BAD_REQUEST);
            }
            Map<String, Object> chunkMap = (Map<String, Object>) chunkObj;
            int startOffset = requireInt(chunkMap, "start_offset");
            int endOffset = requireInt(chunkMap, "end_offset");
            Object embeddingsObj = chunkMap.get("embeddings");
            if (embeddingsObj == null) {
                throw new ElasticsearchStatusException("Each chunk must contain 'embeddings'", RestStatus.BAD_REQUEST);
            }
            BytesReference rawEmbeddings = convertToBytes(embeddingsObj);
            result.add(new SemanticTextField.Chunk(startOffset, endOffset, rawEmbeddings));
        }
        return result;
    }

    private static int requireInt(Map<String, Object> map, String key) {
        Object val = map.get(key);
        if (val == null) {
            throw new ElasticsearchStatusException("Each chunk must contain '{}'", RestStatus.BAD_REQUEST, key);
        }
        if (val instanceof Number n) {
            return n.intValue();
        }
        throw new ElasticsearchStatusException("'{}' must be an integer", RestStatus.BAD_REQUEST, key);
    }

    @SuppressWarnings("unchecked")
    private static BytesReference convertToBytes(Object embeddingsObj) {
        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            if (embeddingsObj instanceof Map<?, ?>) {
                builder.map((Map<String, Object>) embeddingsObj);
            } else if (embeddingsObj instanceof List<?>) {
                // Wrap bare array in {"values": [...]} format
                builder.startObject();
                builder.field("values", embeddingsObj);
                builder.endObject();
            } else {
                throw new ElasticsearchStatusException(
                    "Unsupported embeddings type [{}]",
                    RestStatus.BAD_REQUEST,
                    embeddingsObj.getClass().getSimpleName()
                );
            }
            return BytesReference.bytes(builder);
        } catch (IOException e) {
            throw new ElasticsearchStatusException("Failed to serialize embeddings", RestStatus.BAD_REQUEST, e);
        }
    }

    @SuppressWarnings("unchecked")
    @Nullable
    private static Map<String, Object> getStaged(String fieldName, Map<String, Object> docSourceMap) {
        Map<String, Object> inferenceMetadata = (Map<String, Object>) docSourceMap.get(InferenceMetadataFieldsMapper.NAME);
        if (inferenceMetadata == null) {
            return null;
        }
        Map<String, Object> fieldMeta = (Map<String, Object>) inferenceMetadata.get(fieldName);
        if (fieldMeta == null) {
            return null;
        }
        return (Map<String, Object>) fieldMeta.get(STAGED_KEY);
    }

    private static StagedSemanticField getStagedOrThrow(String fieldName, Map<String, Object> docSourceMap) {
        Map<String, Object> stagedMap = getStaged(fieldName, docSourceMap);
        if (stagedMap == null) {
            throw new ElasticsearchStatusException(
                "Field [{}] has no staged data; use stage_init first",
                RestStatus.BAD_REQUEST,
                fieldName
            );
        }
        return parseStagedFromMap(stagedMap);
    }

    @SuppressWarnings("unchecked")
    private static StagedSemanticField parseStagedFromMap(Map<String, Object> stagedMap) {
        String text = (String) stagedMap.get(StagedSemanticField.TEXT_FIELD);
        Integer textLength = stagedMap.get(StagedSemanticField.TEXT_LENGTH_FIELD) instanceof Number n ? n.intValue() : text.length();
        Instant lastModified = Instant.parse((String) stagedMap.get(StagedSemanticField.LAST_MODIFIED_FIELD));

        List<SemanticTextField.Chunk> chunks = new ArrayList<>();
        Object chunksObj = stagedMap.get(StagedSemanticField.CHUNKS_FIELD);
        if (chunksObj instanceof List<?> chunksList) {
            for (Object item : chunksList) {
                Map<String, Object> chunkMap = (Map<String, Object>) item;
                int startOffset = ((Number) chunkMap.get(StagedSemanticField.START_OFFSET_FIELD)).intValue();
                int endOffset = ((Number) chunkMap.get(StagedSemanticField.END_OFFSET_FIELD)).intValue();
                Object embObj = chunkMap.get(StagedSemanticField.EMBEDDINGS_FIELD);
                BytesReference rawEmbeddings = convertToBytes(embObj);
                chunks.add(new SemanticTextField.Chunk(startOffset, endOffset, rawEmbeddings));
            }
        }
        return new StagedSemanticField(text, textLength, lastModified, chunks);
    }

    @SuppressWarnings("unchecked")
    private static void setStaged(String fieldName, StagedSemanticField staged, Map<String, Object> docSourceMap) {
        Map<String, Object> inferenceMetadata = (Map<String, Object>) docSourceMap.computeIfAbsent(
            InferenceMetadataFieldsMapper.NAME,
            k -> new HashMap<>()
        );
        Map<String, Object> fieldMeta = (Map<String, Object>) inferenceMetadata.computeIfAbsent(fieldName, k -> new HashMap<>());
        fieldMeta.put(STAGED_KEY, stagedToMap(staged));
    }

    private static Map<String, Object> stagedToMap(StagedSemanticField staged) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(StagedSemanticField.TEXT_FIELD, staged.text());
        map.put(StagedSemanticField.TEXT_LENGTH_FIELD, staged.textLength());
        map.put(StagedSemanticField.LAST_MODIFIED_FIELD, staged.lastModified().toString());
        List<Map<String, Object>> chunkList = new ArrayList<>();
        for (SemanticTextField.Chunk chunk : staged.chunks()) {
            Map<String, Object> chunkMap = new LinkedHashMap<>();
            chunkMap.put(StagedSemanticField.START_OFFSET_FIELD, chunk.startOffset());
            chunkMap.put(StagedSemanticField.END_OFFSET_FIELD, chunk.endOffset());
            chunkMap.put(StagedSemanticField.EMBEDDINGS_FIELD, bytesRefToMap(chunk.rawEmbeddings()));
            chunkList.add(chunkMap);
        }
        map.put(StagedSemanticField.CHUNKS_FIELD, chunkList);
        return map;
    }

    private static Object bytesRefToMap(BytesReference bytes) {
        try {
            return org.elasticsearch.common.xcontent.XContentHelper.convertToMap(bytes, false, XContentType.JSON).v2();
        } catch (Exception e) {
            throw new ElasticsearchStatusException("Failed to parse embeddings bytes", RestStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    @SuppressWarnings("unchecked")
    private static void removeStaged(String fieldName, Map<String, Object> docSourceMap) {
        Map<String, Object> inferenceMetadata = (Map<String, Object>) docSourceMap.get(InferenceMetadataFieldsMapper.NAME);
        if (inferenceMetadata != null) {
            Map<String, Object> fieldMeta = (Map<String, Object>) inferenceMetadata.get(fieldName);
            if (fieldMeta != null) {
                fieldMeta.remove(STAGED_KEY);
                if (fieldMeta.isEmpty()) {
                    inferenceMetadata.remove(fieldName);
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static void promoteToCommitted(
        String fieldName,
        StagedSemanticField staged,
        String inferenceId,
        MinimalServiceSettings modelSettings,
        Map<String, Object> docSourceMap
    ) {
        // Build the committed inference metadata structure
        Map<String, List<SemanticTextField.Chunk>> chunkMap = new LinkedHashMap<>();
        chunkMap.put(fieldName, staged.chunks());

        SemanticTextField committed = new SemanticTextField(
            false, // non-legacy format for BYO
            fieldName,
            null,
            new SemanticTextField.InferenceResult(inferenceId, modelSettings, null, chunkMap),
            XContentType.JSON
        );

        // Write the committed structure to inference metadata
        Map<String, Object> inferenceMetadata = (Map<String, Object>) docSourceMap.computeIfAbsent(
            InferenceMetadataFieldsMapper.NAME,
            k -> new HashMap<>()
        );

        // Convert SemanticTextField to a map via XContent serialization
        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            committed.toXContent(builder, null);
            Map<String, Object> committedMap = org.elasticsearch.common.xcontent.XContentHelper.convertToMap(
                BytesReference.bytes(builder),
                false,
                XContentType.JSON
            ).v2();
            inferenceMetadata.put(fieldName, committedMap);
        } catch (IOException e) {
            throw new ElasticsearchStatusException("Failed to serialize committed semantic text", RestStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    private static void validateDimensions(List<SemanticTextField.Chunk> chunks, MinimalServiceSettings modelSettings, String fieldName) {
        if (modelSettings != null && modelSettings.dimensions() != null) {
            int expectedDims = modelSettings.dimensions();
            for (SemanticTextField.Chunk chunk : chunks) {
                BYOSemanticValidator.validateEmbeddingDimensions(
                    chunk.rawEmbeddings(),
                    expectedDims,
                    fieldName,
                    chunk.startOffset(),
                    chunk.endOffset()
                );
            }
        }
    }
}
