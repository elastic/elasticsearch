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

package org.elasticsearch.action.benchmark.start;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.benchmark.competition.CompetitionResult;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Response for a start benchmark request.
 *
 * A benchmark response will contain a mapping of names to results for each competition.
 */
public class BenchmarkStartResponse extends ActionResponse implements Streamable, ToXContent {

    private String         benchmarkId;
    private boolean        verbose;
    private List<String>   errors;
    private volatile State state  = State.RUNNING;

    Map<String, CompetitionResult> competitionResults;

    public BenchmarkStartResponse() {
        this(null);
    }

    public BenchmarkStartResponse(final String benchmarkId) {
        this(benchmarkId, null);
    }

    public BenchmarkStartResponse(final String benchmarkId, final Map<String, CompetitionResult> competitionResults) {
        this.benchmarkId        = benchmarkId;
        this.errors             = new CopyOnWriteArrayList<>();
        this.competitionResults = new ConcurrentHashMap<>();
        if (competitionResults != null && competitionResults.size() > 0) {
            this.competitionResults.putAll(competitionResults);
        }
    }

    public static enum State {
        INITIALIZING((byte) 0),
        RUNNING((byte) 1),
        PAUSED((byte) 2),
        COMPLETED((byte) 3),
        ABORTED((byte) 4),
        FAILED((byte) 5);

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

    /**
     * Id of the benchmark
     * @return  Id of the benchmark
     */
    public String benchmarkId() {
        return benchmarkId;
    }

    /**
     * Sets the benchmark Id
     * @param benchmarkId Benchmark Id
     */
    public void benchmarkId(String benchmarkId) {
        this.benchmarkId = benchmarkId;
    }

    /**
     * Benchmark state
     * @return  Benchmark state
     */
    public State state() {
        return state;
    }

    /**
     * Sets the state of the benchmark
     * @param state State
     */
    public void state(State state) {
        this.state = state;
    }

    /**
     * Possibly replace the existing state with the new state depending on the severity
     * of the new state. More severe states, such as FAILED, will over-write less severe
     * ones, such as COMPLETED.
     * @param newState  New candidate state
     * @return          The merged state
     */
    public State mergeState(State newState) {
        if (state.compareTo(newState) < 0) {
            state = newState;
        }
        return state;
    }

    /**
     * Map of competition names to competition results
     * @return  Map of competition names to competition results
     */
    public Map<String, CompetitionResult> competitionResults() {
        return competitionResults;
    }

    /**
     * Whether to report verbose statistics
     */
    public boolean verbose() {
        return verbose;
    }

    /**
     * Sets whether to report verbose statistics
     */
    public void verbose(boolean verbose) {
        this.verbose = verbose;
    }

    /**
     * Whether the benchmark encountered error conditions
     * @return  Whether the benchmark encountered error conditions
     */
    public boolean hasErrors() {
        return (errors != null && errors.size() > 0);
    }

    /**
     * Error messages
     * @return  Error messages
     */
    public List<String> errors() {
        return this.errors;
    }

    /**
     * Adds error messages to the response
     * @param errors    Error messages
     */
    public void errors(String... errors) {
        for (String e : errors) {
            this.errors.add(e);
        }
    }

    public void errors(List<String> errors) {
        this.errors.addAll(errors);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.BENCHMARK);
        builder.field(Fields.ID, benchmarkId);
        builder.field(Fields.STATUS, state.toString());
        if (errors != null) {
            builder.array(Fields.ERRORS, errors.toArray(new String[errors.size()]));
        }
        builder.startObject(Fields.COMPETITORS);
        if (competitionResults != null) {
            for (Map.Entry<String, CompetitionResult> entry : competitionResults.entrySet()) {
                entry.getValue().toXContent(builder, params);
            }
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        benchmarkId = in.readOptionalString();
        verbose     = in.readBoolean();
        state       = State.fromId(in.readByte());
        int size    = in.readVInt();
        errors      = new CopyOnWriteArrayList<>();
        for (int i = 0; i < size; i++) {
            final String s = in.readOptionalString();
            if (s != null) {
                errors.add(s);
            }
        }
        size = in.readVInt();
        competitionResults = new ConcurrentHashMap<>(size);
        for (int i = 0; i < size; i++) {
            String s = in.readString();
            CompetitionResult cr = new CompetitionResult();
            cr.readFrom(in);
            cr.verbose(verbose);
            competitionResults.put(s, cr);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(benchmarkId);
        out.writeBoolean(verbose);
        out.writeByte(state.id());
        out.writeVInt(errors.size());
        for (String s : errors) {
            out.writeOptionalString(s);
        }
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
        static final XContentBuilderString BENCHMARK   = new XContentBuilderString("benchmark");
        static final XContentBuilderString ID          = new XContentBuilderString("id");
        static final XContentBuilderString STATUS      = new XContentBuilderString("status");
        static final XContentBuilderString ERRORS      = new XContentBuilderString("errors");
        static final XContentBuilderString COMPETITORS = new XContentBuilderString("competitors");
    }
}
