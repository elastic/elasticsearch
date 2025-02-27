/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.io.stream;

import org.elasticsearch.TransportVersion;

import java.io.IOException;
import java.util.Map;

/**
 * A {@link NamedWriteable} that has a minimum version associated with it.
 */
public interface VersionedNamedWriteable extends NamedWriteable {

    /**
     * Returns the name of the writeable object
     */
    String getWriteableName();

    /**
     * The minimal version of the recipient this object can be sent to
     */
    TransportVersion getMinimalSupportedVersion();

    /**
     * Tests whether or not the custom should be serialized. The criteria is the output stream must be at least the minimum supported
     * version of the custom. That is, we only serialize customs to clients than can understand the custom based on the version of the
     * client.
     *
     * @param out    the output stream
     * @param custom the custom to serialize
     * @param <T>    the type of the custom
     * @return true if the custom should be serialized and false otherwise
     */
    static <T extends VersionedNamedWriteable> boolean shouldSerialize(final StreamOutput out, final T custom) {
        return out.getTransportVersion().onOrAfter(custom.getMinimalSupportedVersion());
    }

    /**
     * Writes all those values in the given map to {@code out} that pass the version check in {@link #shouldSerialize} as a list.
     *
     * @param out     stream to write to
     * @param customs map of customs
     * @param <T>     type of customs in map
     */
    static <T extends VersionedNamedWriteable> void writeVersionedWritables(StreamOutput out, Map<String, T> customs) throws IOException {
        writeVersionedWriteables(out, customs.values());
    }

    static void writeVersionedWriteables(StreamOutput out, Iterable<? extends VersionedNamedWriteable> writeables) throws IOException {
        // filter out objects not supported by the stream version
        int numberOfCompatibleValues = 0;
        for (var value : writeables) {
            if (shouldSerialize(out, value)) {
                numberOfCompatibleValues++;
            }
        }
        out.writeVInt(numberOfCompatibleValues);
        for (var value : writeables) {
            if (shouldSerialize(out, value)) {
                out.writeNamedWriteable(value);
            }
        }
    }
}
