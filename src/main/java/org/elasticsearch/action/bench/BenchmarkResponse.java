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

package org.elasticsearch.action.bench;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.*;

/**
 * Benchmark response.
 *
 * A benchmark response will contain a mapping of names to results for each competition.
 */
public class BenchmarkResponse extends ActionResponse implements Streamable, ToXContent {

    private String benchmarkName;
    private State state = State.RUNNING;
    private boolean verbose;

    Map<String, CompetitionResult> competitionResults;

    public BenchmarkResponse() {
        competitionResults = new HashMap<String, CompetitionResult>();
    }

    public BenchmarkResponse(String benchmarkName, Map<String, CompetitionResult> competitionResults) {
        this.benchmarkName = benchmarkName;
        this.competitionResults = competitionResults;
    }

    /**
     * Benchmarks can be in one of:
     *  RUNNING     - executing normally
     *  COMPLETE    - completed normally
     *  ABORTED     - aborted
     */
    public static enum State {
        RUNNING((byte) 0),
        COMPLETE((byte) 1),
        ABORTED((byte) 2);

        private final byte id;
        private static final State[] STATES = new State[State.values().length];

        static {
            for (State state : State.values()) {
                assert state.id() < STATES.length && state.id() >= 0;
                STATES[state.id] = state;
            }
        }

        State(byte id) {
            this.id = id;
        }

        public byte id() {
            return id;
        }

        public static State fromId(byte id) throws ElasticsearchIllegalArgumentException {
            if (id < 0 || id >= STATES.length) {
                throw new ElasticsearchIllegalArgumentException("No mapping for id [" + id + "]");
            }
            return STATES[id];
        }
    }

    public String benchmarkName() {
        return benchmarkName;
    }

    public void benchmarkName(String benchmarkName) {
        this.benchmarkName = benchmarkName;
    }

    public State state() {
        return state;
    }

    public void state(State state) {
        this.state = state;
    }

    public Map<String, CompetitionResult> competitionResults() {
        return competitionResults;
    }

    public boolean isAborted() {
        return state == State.ABORTED;
    }

    public boolean hasFailures() {
        for (CompetitionResult result : competitionResults.values()) {
            if (result.hasFailures()) {
                return true;
            }
        }
        return false;
    }

    public boolean verbose() {
        return verbose;
    }

    public void verbose(boolean verbose) {
        this.verbose = verbose;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(Fields.STATUS, state.toString());
        builder.startObject(Fields.COMPETITORS);
        if (competitionResults != null) {
            for (Map.Entry<String, CompetitionResult> entry : competitionResults.entrySet()) {
                entry.getValue().verbose(verbose);
                entry.getValue().toXContent(builder, params);
            }
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        benchmarkName = in.readString();
        state = State.fromId(in.readByte());
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            String s = in.readString();
            CompetitionResult cr = new CompetitionResult();
            cr.readFrom(in);
            competitionResults.put(s, cr);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(benchmarkName);
        out.writeByte(state.id());
        out.write(competitionResults.size());
        for (Map.Entry<String, CompetitionResult> entry : competitionResults.entrySet()) {
            out.writeString(entry.getKey());
            entry.getValue().writeTo(out);
        }
    }

    @Override
    public String toString() {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
            builder.startObject();
            toXContent(builder, EMPTY_PARAMS);
            builder.endObject();
            return builder.string();
        } catch (IOException e) {
            return "{ \"error\" : \"" + e.getMessage() + "\"}";
        }
    }

    static final class Fields {
        static final XContentBuilderString STATUS = new XContentBuilderString("status");
        static final XContentBuilderString COMPETITORS = new XContentBuilderString("competitors");
    }
}
