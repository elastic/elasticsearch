/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.hash.MurmurHash3;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.TreeMap;

/**
 * ID Generator for creating unique but deterministic document ids.
 *
 * uses MurmurHash with 128 bits
 */
public final class IDGenerator {
    private static final byte[] NULL_VALUE = "__NULL_VALUE__".getBytes(StandardCharsets.UTF_8);
    private static final byte[] EMPTY_VALUE = "__EMPTY_VALUE__".getBytes(StandardCharsets.UTF_8);
    private static final byte DELIM = '$';
    private static final long SEED = 19;
    private static final int MAX_FIRST_BYTES = 5;

    private final TreeMap<String, Object> objectsForIDGeneration = new TreeMap<>();

    public IDGenerator() {
    }

    /**
     * Add a value to the generator
     * @param key object identifier, to be used for consistent sorting
     * @param value the value
     */
    public void add(String key, Object value) {
        if (objectsForIDGeneration.containsKey(key)) {
            throw new IllegalArgumentException("Keys must be unique");
        }
        objectsForIDGeneration.put(key, value);
    }

    /**
     * Create a document id based on the input objects
     *
     * @return a document id as string
     */
    public String getID() {
        if (objectsForIDGeneration.size() == 0) {
            throw new RuntimeException("Add at least 1 object before generating the ID");
        }

        BytesRefBuilder buffer = new BytesRefBuilder();
        BytesRefBuilder hashedBytes = new BytesRefBuilder();

        for (Object value : objectsForIDGeneration.values()) {
            byte[] v = getBytes(value);
            if (v.length == 0) {
                v = EMPTY_VALUE;
            }
            buffer.append(v, 0, v.length);
            buffer.append(DELIM);

            // keep the 1st byte of every object
            if (hashedBytes.length() <= MAX_FIRST_BYTES) {
                hashedBytes.append(v[0]);
            }
        }
        MurmurHash3.Hash128 hasher = MurmurHash3.hash128(buffer.bytes(), 0, buffer.length(), SEED, new MurmurHash3.Hash128());
        hashedBytes.append(Numbers.longToBytes(hasher.h1), 0, 8);
        hashedBytes.append(Numbers.longToBytes(hasher.h2), 0, 8);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(hashedBytes.bytes());
    }

    /**
     * Turns objects into byte arrays, only supporting types returned groupBy
     *
     * @param value the value as object
     * @return a byte representation of the input object
     */
    private static byte[] getBytes(Object value) {
        if (value == null) {
            return NULL_VALUE;
        } else if (value instanceof String) {
            return ((String) value).getBytes(StandardCharsets.UTF_8);
        } else if (value instanceof Long) {
            return Numbers.longToBytes((Long) value);
        } else if (value instanceof Double) {
            return Numbers.doubleToBytes((Double) value);
        } else if (value instanceof Integer) {
            return Numbers.intToBytes((Integer) value);
        }

        throw new IllegalArgumentException("Value of type [" + value.getClass() + "] is not supported");
    }
}
