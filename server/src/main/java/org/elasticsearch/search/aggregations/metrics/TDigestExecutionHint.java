/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Indicates which implementation is used in TDigestState.
 */
public enum TDigestExecutionHint implements Writeable {
    DEFAULT(0),        // Use a TDigest that is optimized for performance, with a small penalty in accuracy.
    HIGH_ACCURACY(1);  // Use a TDigest that is optimize for accuracy, at the expense of performance.

    TDigestExecutionHint(int id) {
        this.id = id;
    }

    // ID needs to be unique. Updating the id of a value is a backwards-incompatible change.
    private final int id;

    /**
     * Case-insensitive wrapper of valueOf()
     * @param value input string value
     * @return an ExecutionHint
     */
    public static TDigestExecutionHint parse(String value) {
        try {
            return switch (value) {
                case "high_accuracy" -> HIGH_ACCURACY;
                case "default", "" -> DEFAULT;
                default -> valueOf(value);
            };
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid execution_hint [" + value + "], valid values are [default, high_accuracy]");
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(id);
    }

    public static TDigestExecutionHint readFrom(StreamInput in) throws IOException {
        int value = in.readVInt();
        if (value == DEFAULT.id) {
            return DEFAULT;
        }
        if (value == HIGH_ACCURACY.id) {
            return HIGH_ACCURACY;
        }
        throw new IllegalStateException("Received unknown TDigestExecutionHint id: " + value);
    }
}
