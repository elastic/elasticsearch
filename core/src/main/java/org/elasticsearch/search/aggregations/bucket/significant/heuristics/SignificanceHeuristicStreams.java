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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * A registry for all significance heuristics. This is needed for reading them from a stream without knowing which
 * one it is.
 */
public class SignificanceHeuristicStreams {

    private static Map<String, SignificanceHeuristic> STREAMS = Collections.emptyMap();

    static {
        HashMap<String, SignificanceHeuristic> map = new HashMap<>();
        map.put(JLHScore.NAMES_FIELD.getPreferredName(), JLHScore.PROTOTYPE);
        map.put(PercentageScore.NAMES_FIELD.getPreferredName(), PercentageScore.PROTOTYPE);
        map.put(MutualInformation.NAMES_FIELD.getPreferredName(), MutualInformation.PROTOTYPE);
        map.put(GND.NAMES_FIELD.getPreferredName(), GND.PROTOTYPE);
        map.put(ChiSquare.NAMES_FIELD.getPreferredName(), ChiSquare.PROTOTYPE);
        map.put(ScriptHeuristic.NAMES_FIELD.getPreferredName(), ScriptHeuristic.PROTOTYPE);
        STREAMS = Collections.unmodifiableMap(map);
    }

    public static SignificanceHeuristic read(StreamInput in) throws IOException {
        return stream(in.readString()).readFrom(in);
    }

    public static void writeTo(SignificanceHeuristic significanceHeuristic, StreamOutput out) throws IOException {
        out.writeString(significanceHeuristic.getWriteableName());
        significanceHeuristic.writeTo(out);
    }

    /**
     * A stream that knows how to read an heuristic from the input.
     */
    public static interface Stream {

        SignificanceHeuristic readResult(StreamInput in) throws IOException;

        String getName();
    }

    /**
     * Registers the given prototype.
     *
     * @param prototype
     *            The prototype to register
     */
    public static synchronized void registerPrototype(SignificanceHeuristic prototype) {
        if (STREAMS.containsKey(prototype.getWriteableName())) {
            throw new IllegalArgumentException("Can't register stream with name [" + prototype.getWriteableName() + "] more than once");
        }
        HashMap<String, SignificanceHeuristic> map = new HashMap<>();
        map.putAll(STREAMS);
        map.put(prototype.getWriteableName(), prototype);
        STREAMS = Collections.unmodifiableMap(map);
    }

    /**
     * Returns the stream that is registered for the given name
     *
     * @param name The given name
     * @return The associated stream
     */
    private static synchronized SignificanceHeuristic stream(String name) {
        return STREAMS.get(name);
    }

}
