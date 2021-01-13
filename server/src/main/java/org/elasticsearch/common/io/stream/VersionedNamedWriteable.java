/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.io.stream;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.Version;
import org.elasticsearch.common.collect.ImmutableOpenMap;

import java.io.IOException;

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
    Version getMinimalSupportedVersion();

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
        return out.getVersion().onOrAfter(custom.getMinimalSupportedVersion());
    }

    /**
     * Writes all those values in the given map to {@code out} that pass the version check in {@link #shouldSerialize} as a list.
     *
     * @param out     stream to write to
     * @param customs map of customs
     * @param <T>     type of customs in map
     */
    static <T extends VersionedNamedWriteable> void writeVersionedWritables(StreamOutput out, ImmutableOpenMap<String, T> customs)
            throws IOException {
        // filter out custom states not supported by the other node
        int numberOfCustoms = 0;
        for (final ObjectCursor<T> cursor : customs.values()) {
            if (shouldSerialize(out, cursor.value)) {
                numberOfCustoms++;
            }
        }
        out.writeVInt(numberOfCustoms);
        for (final ObjectCursor<T> cursor : customs.values()) {
            if (shouldSerialize(out, cursor.value)) {
                out.writeNamedWriteable(cursor.value);
            }
        }
    }
}
