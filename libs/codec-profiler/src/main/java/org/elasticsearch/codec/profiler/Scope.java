/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.codec.profiler;

import java.util.Map;

/**
 * Gives a recorder to each reader opened while the scope is bound. A scope covers a part of the work that a frontend
 * profiles.
 *
 * <p>The frontend binds a scope, not a collector, so that it can create a collector only when a codec opens a reader.
 * Work that opens no reader allocates and reports nothing.
 */
@FunctionalInterface
public interface Scope {

    /**
     * The recorder for a reader that is opening: a reader of {@code field} in segment {@code segment}, by the doc values
     * format {@code codec}. It is called once for each reader opened in the scope, on the thread that opens the reader,
     * never on the read path. The frontend thus decides once for each reader, not for each event, where the events go.
     *
     * <p>An implementation that creates collectors lazily returns the same recorder for the same arguments within one
     * execution partition. Segment names are unique within one shard only, so a frontend that reads several shards
     * changes partition when it moves to another shard. The same arguments then name another segment and get another
     * recorder.
     *
     * @param codec   the name of the doc values format that opens the reader; it is the codec's identity in a profile
     * @param segment the name of the segment the reader reads, unique within one index shard only
     * @param field   the name of the field, as the index knows it, that the reader reads
     * @param facts   what the codec knows about the stored column the reader reads, such as whether it has a skip
     *                index: an immutable map, often empty, that the frontend reports but does not interpret. These are
     *                descriptions, not measurements.
     */
    MetricRecorder metrics(String codec, String segment, String field, Map<String, Boolean> facts);
}
