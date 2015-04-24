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
package org.elasticsearch.search.aggregations.bucket.significant.heuristics;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.Version;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * A registry for all significance heuristics. This is needed for reading them from a stream without knowing which
 * one it is.
 */
public class SignificanceHeuristicStreams {

    private static ImmutableMap<String, Stream> STREAMS = ImmutableMap.of();

    public static SignificanceHeuristic read(StreamInput in) throws IOException {
        return stream(in.readString()).readResult(in);
    }

    /**
     * A stream that knows how to read an heuristic from the input.
     */
    public static interface Stream {

        SignificanceHeuristic readResult(StreamInput in) throws IOException;

        String getName();
    }

    /**
     * Registers the given stream and associate it with the given types.
     *
     * @param stream The stream to register
     * @param names  The names associated with the streams
     */
    public static synchronized void registerStream(Stream stream, String... names) {
        MapBuilder<String, Stream> uStreams = MapBuilder.newMapBuilder(STREAMS);
        for (String name : names) {
            uStreams.put(name, stream);
        }
        STREAMS = uStreams.immutableMap();
    }

    /**
     * Returns the stream that is registered for the given name
     *
     * @param name The given name
     * @return The associated stream
     */
    public static Stream stream(String name) {
        return STREAMS.get(name);
    }

}
