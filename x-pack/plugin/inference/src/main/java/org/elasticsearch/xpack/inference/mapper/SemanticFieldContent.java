/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.elasticsearch.inference.InferenceString;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.mapper.OffsetSourceFieldMapper.OffsetSource;

public class SemanticFieldContent {
    private final List<String> textValues;
    private final Map<Integer, Map<?, ?>> mapValues;

    public SemanticFieldContent(Object fieldValue) {
        if (fieldValue == null) {
            this.textValues = List.of();
            this.mapValues = Map.of();
        } else if (fieldValue instanceof List<?> list) {
            this.textValues = new ArrayList<>(list.size());
            this.mapValues = new HashMap<>(list.size());
            parseFieldValues(list, textValues, mapValues);
        } else {
            this.textValues = new ArrayList<>(1);
            this.mapValues = new HashMap<>(1);
            parseFieldValues(List.of(fieldValue), textValues, mapValues);
        }
    }

    /**
     * Resolves the content referenced by the given {@link OffsetSource}.
     * Returns a {@link Map} for object inputs (representing an {@link InferenceString}) or a {@link String} for text chunk inputs.
     *
     * @param offset The offset to use the resolve the content
     * @return The resolved content
     * @throws IllegalArgumentException If the offset refers to an object input, but the referenced object is missing
     * @throws IndexOutOfBoundsException If the offset refers to a text input, but the start or end offset is out of bounds
     */
    public Object resolve(OffsetSource offset) {
        if (offset.inputIndex() != null) {
            Map<?, ?> mapValue = getMapValue(offset.inputIndex());
            if (mapValue == null) {
                throw new IllegalArgumentException("Missing object value at input index [" + offset.inputIndex() + "]");
            }
            return mapValue;
        }

        return getChunkText(offset.start(), offset.end());
    }

    /**
     * Resolves a {@code (startOffset, endOffset)} pair back to the original substring of one of the underlying text values.
     * <p>
     * Offsets are expressed against a <em>virtual concatenation</em> of all string inputs, with a single separator character inserted
     * between adjacent values. This matches the representation produced by
     * {@code ShardBulkInferenceActionFilter#addInferenceRequestsForSourceFieldValues}, which accumulates an {@code offsetAdjustment} of
     * {@code string.length() + 1} per value so that every text chunk offset is global within that virtual string.
     * <p>
     * Because inference never produces chunks that straddle two source values, this method rejects spans that cross a value boundary
     * or whose start falls on a separator position.
     *
     * @param startOffset the start offset (inclusive) of the chunk within the virtual concatenated string
     * @param endOffset   the end offset (exclusive) of the chunk within the virtual concatenated string
     * @return the substring of the source text value identified by the offsets
     * @throws IndexOutOfBoundsException if the start or end offset is invalid
     */
    String getChunkText(int startOffset, int endOffset) {
        if (textValues.isEmpty()) {
            throw new IndexOutOfBoundsException("Chunk text offset [" + startOffset + ", " + endOffset + "] is out of bounds");
        }

        // Find the text value that corresponds with the offest
        int textValueIndex = 0;
        int currentStartOffset = startOffset;
        int currentEndOffset = endOffset;
        String currentTextValue = textValues.get(textValueIndex);
        while (currentStartOffset >= currentTextValue.length()) {
            if (++textValueIndex >= textValues.size()) {
                throw new IndexOutOfBoundsException("Chunk text offset [" + startOffset + ", " + endOffset + "] is out of bounds");
            }

            // Add one to account for separator character between text values
            int offsetAdjustment = currentTextValue.length() + 1;
            currentStartOffset -= offsetAdjustment;
            currentEndOffset -= offsetAdjustment;
            if (currentStartOffset < 0) {
                throw new IndexOutOfBoundsException("Start offset [" + startOffset + "] refers to a separator character");
            }

            currentTextValue = textValues.get(textValueIndex);
        }

        if (currentEndOffset > currentTextValue.length()) {
            throw new IndexOutOfBoundsException("End offset [" + endOffset + "] crosses a text value boundary");
        }

        return currentTextValue.substring(currentStartOffset, currentEndOffset);
    }

    Map<?, ?> getMapValue(int inputIndex) {
        var mapValue = mapValues.get(inputIndex);
        if (mapValue == null) {
            return null;
        } else {
            return Collections.unmodifiableMap(mapValue);
        }
    }

    private static void parseFieldValues(List<?> fieldValues, List<String> textValues, Map<Integer, Map<?, ?>> mapValues) {
        int valueIndex = 0;
        for (Object value : fieldValues) {
            if (value instanceof Map<?, ?> map) {
                mapValues.put(valueIndex, map);
            } else {
                textValues.add(value.toString());
            }

            valueIndex++;
        }
    }
}
