/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.streaming;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;

/**
 * Parses a stream of bytes that form a JSON array, where each element of the array
 * is a JSON object. This parser extracts each complete JSON object from the array
 * and emits it as byte array.
 *
 * Example of an expected stream:
 * Chunk 1: [{"key":"val1"}
 * Chunk 2: ,{"key2":"val2"}
 * Chunk 3: ,{"key3":"val3"}, {"some":"object"}]
 *
 * This parser would emit four byte arrays, with data:
 * 1. {"key":"val1"}
 * 2. {"key2":"val2"}
 * 3. {"key3":"val3"}
 * 4. {"some":"object"}
 */
public class JsonArrayPartsEventParser {

    // Buffer to hold bytes from the previous call if they formed an incomplete JSON object.
    private final ByteArrayOutputStream incompletePart = new ByteArrayOutputStream();

    public Deque<byte[]> parse(byte[] newBytes) {
        if (newBytes == null || newBytes.length == 0) {
            return new ArrayDeque<>(0);
        }

        ByteArrayOutputStream currentStream = new ByteArrayOutputStream();
        try {
            currentStream.write(incompletePart.toByteArray());
            currentStream.write(newBytes);
        } catch (IOException e) {
            throw new UncheckedIOException("Error handling byte array streams", e);
        }
        incompletePart.reset();

        byte[] dataToProcess = currentStream.toByteArray();
        return parseInternal(dataToProcess);
    }

    private Deque<byte[]> parseInternal(byte[] data) {
        int localBraceLevel = 0;
        int objectStartIndex = -1;
        Deque<byte[]> completedObjects = new ArrayDeque<>();

        for (int i = 0; i < data.length; i++) {
            char c = (char) data[i];

            if (c == '{') {
                if (localBraceLevel == 0) {
                    objectStartIndex = i;
                }
                localBraceLevel++;
            } else if (c == '}') {
                if (localBraceLevel > 0) {
                    localBraceLevel--;
                    if (localBraceLevel == 0) {
                        byte[] jsonObject = Arrays.copyOfRange(data, objectStartIndex, i + 1);
                        completedObjects.offer(jsonObject);
                        objectStartIndex = -1;
                    }
                }
            }
        }

        if (localBraceLevel > 0) {
            incompletePart.write(data, objectStartIndex, data.length - objectStartIndex);
        }
        return completedObjects;
    }
}
