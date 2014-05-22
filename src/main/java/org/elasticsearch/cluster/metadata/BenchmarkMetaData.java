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
package org.elasticsearch.cluster.metadata;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.*;

/**
 * Meta data about benchmarks that are currently executing
 */
public class BenchmarkMetaData implements MetaData.Custom {

    public static final String TYPE = "benchmark";
    public static final Factory FACTORY = new Factory();

    private final ImmutableList<Entry> entries;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BenchmarkMetaData that = (BenchmarkMetaData) o;
        if (!entries.equals(that.entries)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return entries.hashCode();
    }

    public static ImmutableList<Entry> delta(BenchmarkMetaData prev, BenchmarkMetaData cur) {

        if (cur == null || cur.entries == null || cur.entries.size() == 0) {
            return ImmutableList.of();
        }
        if (prev == null || prev.entries == null || prev.entries.size() == 0) {
            return ImmutableList.copyOf(cur.entries);
        }

        final List<Entry> changed = new ArrayList<>();

        for (Entry e : cur.entries) {
            final Entry matched = find(e, prev.entries);
            if (matched == null) {
                changed.add(e);
            } else {
                if (!e.equals(matched)) {
                    changed.add(e);
                }
            }
        }

        return ImmutableList.copyOf(changed);
    }

    private static Entry find(Entry entry, List<Entry> list) {
        for (Entry e : list) {
            if (e.benchmarkId().equals(entry.benchmarkId())) {
                return e;
            }
        }
        return null;
    }

    public static class Entry {

        private final String                 benchmarkId;
        private final State                  state;
        private final Map<String, NodeState> nodeStateMap;

        public Entry(String benchmarkId) {
            this(benchmarkId, State.INITIALIZING, new HashMap<String, NodeState>());
        }

        public Entry(String benchmarkId, State state, Map<String, NodeState> nodeStateMap) {
            this.benchmarkId  = benchmarkId;
            this.state        = state;
            this.nodeStateMap = nodeStateMap;
        }

        public String benchmarkId() {
            return benchmarkId;
        }

        public State state() {
            return state;
        }

        public Map<String, NodeState> nodeStateMap() {
            return nodeStateMap;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Entry entry = (Entry) o;

            if (!benchmarkId.equals(entry.benchmarkId)) return false;
            if (state != entry.state) return false;

            if (nodeStateMap().size() != entry.nodeStateMap().size()) return false;

            for (Map.Entry<String, NodeState> me : nodeStateMap().entrySet()) {
                final NodeState ns = entry.nodeStateMap().get(me.getKey());
                if (ns == null) {
                    return false;
                }
                if (me.getValue() != ns) {
                    return false;
                }
            }

            return true;
        }

        @Override
        public int hashCode() {
            int result = benchmarkId.hashCode();
            result = 31 * result + state.hashCode();
            return result;
        }

        public static enum NodeState {

            INITIALIZING((byte) 0),
            READY((byte) 1),
            RUNNING((byte) 2),
            PAUSED((byte) 3),
            COMPLETED((byte) 4),
            FAILED((byte) 5),
            ABORTED((byte) 6);

            private static final NodeState[] NODE_STATES = new NodeState[NodeState.values().length];

            static {
                for (NodeState state : NodeState.values()) {
                    assert state.id() < NODE_STATES.length && state.id() >= 0;
                    NODE_STATES[state.id()] = state;
                }
            }

            private final byte id;

            NodeState(byte id) {
                this.id = id;
            }

            public byte id() {
                return id;
            }

            public static NodeState fromId(byte id) {
                if (id < 0 || id >= NodeState.values().length) {
                    throw new ElasticsearchIllegalArgumentException("No benchmark state for value [" + id + "]");
                }
                return NODE_STATES[id];
            }
        }
    }

    public static enum State {

        INITIALIZING((byte) 0),
        RUNNING((byte) 1),
        STARTED((byte) 2),
        PAUSED((byte) 3),
        RESUMING((byte) 4),
        COMPLETED((byte) 5),
        FAILED((byte) 6),
        ABORTED((byte) 7);

        private static final State[] STATES = new State[State.values().length];

        static {
            for (State state : State.values()) {
                assert state.id() < STATES.length && state.id() >= 0;
                STATES[state.id()] = state;
            }
        }

        private final byte id;

        State(byte id) {
            this.id = id;
        }

        public byte id() {
            return id;
        }

        public boolean completed() {
            return this == COMPLETED || this == FAILED;
        }

        public static State fromId(byte id) {
            if (id < 0 || id >= State.values().length) {
                throw new ElasticsearchIllegalArgumentException("No benchmark state for value [" + id + "]");
            }
            return STATES[id];
        }
    }

    public BenchmarkMetaData(ImmutableList<Entry> entries) {
        this.entries = entries;
    }

    public BenchmarkMetaData(Entry... entries) {
        this.entries = ImmutableList.copyOf(entries);
    }

    public ImmutableList<Entry> entries() {
        return this.entries;
    }

    public static class Factory implements MetaData.Custom.Factory<BenchmarkMetaData> {

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        public BenchmarkMetaData readFrom(StreamInput in) throws IOException {
            Entry[] entries = new Entry[in.readVInt()];
            for (int i = 0; i < entries.length; i++) {
                String benchmarkId = in.readString();
                State state = State.fromId(in.readByte());
                int size = in.readVInt();
                Map<String, Entry.NodeState> map = new HashMap<>(size);
                for (int j = 0; j < size; j++) {
                    map.put(in.readString(), Entry.NodeState.fromId(in.readByte()));
                }
                entries[i] = new Entry(benchmarkId, state, map);
            }
            return new BenchmarkMetaData(entries);
        }

        @Override
        public void writeTo(BenchmarkMetaData repositories, StreamOutput out) throws IOException {
            out.writeVInt(repositories.entries().size());
            for (Entry entry : repositories.entries()) {
                out.writeString(entry.benchmarkId());
                out.writeByte(entry.state().id());
                out.writeVInt(entry.nodeStateMap.size());
                for (Map.Entry<String, Entry.NodeState> mapEntry : entry.nodeStateMap().entrySet()) {
                    out.writeString(mapEntry.getKey());
                    out.writeByte(mapEntry.getValue().id());
                }
            }
        }

        @Override
        public BenchmarkMetaData fromXContent(XContentParser parser) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void toXContent(BenchmarkMetaData customIndexMetaData, XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startArray("benchmarks");
            for (Entry entry : customIndexMetaData.entries()) {
                toXContent(entry, builder, params);
            }
            builder.endArray();
        }

        public void toXContent(Entry entry, XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject();
            builder.field("id", entry.benchmarkId());
            builder.field("state", entry.state());
            builder.startArray("node_states");
            for (Map.Entry<String, Entry.NodeState> mapEntry : entry.nodeStateMap().entrySet()) {
                builder.field("node_id", mapEntry.getKey());
                builder.field("node_state", mapEntry.getValue());
            }
            builder.endArray();
            builder.endObject();
        }

        public boolean isPersistent() {
            return false;
        }
    }

    public boolean contains(String benchmarkId) {
        for (Entry e : entries) {
           if (e.benchmarkId.equals(benchmarkId)) {
               return true;
           }
        }
        return false;
    }

    public Entry get(String benchmarkId) {
        for (Entry e : entries) {
            if (e.benchmarkId.equals(benchmarkId)) {
                return e;
            }
        }
        return null;
    }
}
