/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.support.MapXContentParser;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    public String getChunkText(int startOffset, int endOffset) {
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

    public Map<?, ?> getMapValue(int inputIndex) {
        var mapValue = mapValues.get(inputIndex);
        if (mapValue == null) {
            return null;
        } else {
            return Collections.unmodifiableMap(mapValue);
        }
    }

    public InferenceString getInferenceStringValue(int inputIndex) {
        Map<?, ?> mapValue = getMapValue(inputIndex);
        if (mapValue == null) {
            return null;
        }

        return parseInferenceStringValue(mapValue);
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

    private static InferenceString parseInferenceStringValue(Map<?, ?> value) {
        @SuppressWarnings("unchecked")
        Map<String, Object> stringKeyedMap = (Map<String, Object>) value;

        try (
            XContentParser parser = new MapXContentParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.IGNORE_DEPRECATIONS,
                stringKeyedMap,
                XContentType.JSON
            )
        ) {
            return InferenceString.PARSER.parse(parser, null);
        } catch (Exception e) {
            throw new IllegalArgumentException("Cannot parse value [" + value + "] to an InferenceString", e);
        }
    }
}
