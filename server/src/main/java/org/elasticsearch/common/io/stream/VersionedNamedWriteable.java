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
     * The minimal version of the recipient this object can be sent to.
     * See {@link #supportsVersion(TransportVersion)} for the default serialization check.
     */
    TransportVersion getMinimalSupportedVersion();

    /**
     * Determines whether this instance should be serialized based on the provided transport version.
     *
     * The default implementation returns {@code true} if the given transport version is
     * equal to or newer than {@link #getMinimalSupportedVersion()}.
     * Subclasses may override this method to define custom serialization logic.
     *
     * @param version the transport version of the receiving node
     * @return {@code true} if the instance should be serialized, {@code false} otherwise
     */
    default boolean supportsVersion(TransportVersion version) {
        return version.onOrAfter(getMinimalSupportedVersion());
    }

    /**
     * Writes all those values in the given map to {@code out} that pass the version check in
     * {@link VersionedNamedWriteable#supportsVersion} as a list.
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
            if (value.supportsVersion(out.getTransportVersion())) {
                numberOfCompatibleValues++;
            }
        }
        out.writeVInt(numberOfCompatibleValues);
        for (var value : writeables) {
            if (value.supportsVersion(out.getTransportVersion())) {
                out.writeNamedWriteable(value);
            }
        }
    }
}
