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

import org.elasticsearch.common.collect.CopyOnWriteHashMap;
import org.elasticsearch.common.io.stream.StreamInput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * A registry for all significance heuristics. This is needed for reading them from a stream without knowing which
 * one it is.
 */
public class SignificanceHeuristicStreams {

    private static Map<String, Stream> STREAMS = Collections.EMPTY_MAP;

    static {
        HashMap<String, Stream> map = new HashMap<>();
        map.put(JLHScore.STREAM.getName(), JLHScore.STREAM);
        map.put(PercentageScore.STREAM.getName(), PercentageScore.STREAM);
        map.put(MutualInformation.STREAM.getName(), MutualInformation.STREAM);
        map.put(GND.STREAM.getName(), GND.STREAM);
        map.put(ChiSquare.STREAM.getName(), ChiSquare.STREAM);
        map.put(ScriptHeuristic.STREAM.getName(), ScriptHeuristic.STREAM);
        STREAMS = Collections.unmodifiableMap(map);
    }

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
     */
    public static synchronized void registerStream(Stream stream) {
        if (STREAMS.containsKey(stream.getName())) {
            throw new IllegalArgumentException("Can't register stream with name [" + stream.getName() + "] more than once");
        }
        HashMap<String, Stream> map = new HashMap<>();
        map.putAll(STREAMS);
        map.put(stream.getName(), stream);
        STREAMS = Collections.unmodifiableMap(map);
    }

    /**
     * Returns the stream that is registered for the given name
     *
     * @param name The given name
     * @return The associated stream
     */
    public static synchronized Stream stream(String name) {
        return STREAMS.get(name);
    }

}
