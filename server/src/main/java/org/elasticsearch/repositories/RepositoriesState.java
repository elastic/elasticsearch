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

package org.elasticsearch.repositories;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Tracks the current generation at which the {@link RepositoryData} for each writable
 * {@link org.elasticsearch.repositories.blobstore.BlobStoreRepository} in the cluster can be found.
 * See documentation for the package {@link org.elasticsearch.repositories.blobstore} for details.
 */
public final class RepositoriesState extends AbstractNamedDiffable<ClusterState.Custom> implements ClusterState.Custom {

    public static final Version REPO_GEN_IN_CS_VERSION = Version.V_8_0_0;

    public static final String TYPE = "repositories_state";

    private static final RepositoriesState EMPTY = RepositoriesState.builder().build();

    private final Map<String, State> states;

    private RepositoriesState(Map<String, State> states) {
        this.states = states;
    }

    public RepositoriesState(StreamInput in) throws IOException {
        this(in.readMap(StreamInput::readString, State::new));
    }

    /**
     * Gets repositories state from the given cluster state or an empty repositories state if none is found in the cluster state.
     *
     * @param state cluster state
     * @return RepositoriesState
     */
    public static RepositoriesState getOrEmpty(ClusterState state) {
        return Objects.requireNonNullElse(state.custom(RepositoriesState.TYPE), RepositoriesState.EMPTY);
    }

    /**
     * Get {@link State} for a repository.
     *
     * @param repoName repository name
     * @return State for the repository or {@code null} if none is found
     */
    @Nullable
    public State state(String repoName) {
        return states.get(repoName);
    }

    public static NamedDiff<ClusterState.Custom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(ClusterState.Custom.class, TYPE, in);
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return REPO_GEN_IN_CS_VERSION;
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(states, StreamOutput::writeString, (o, v) -> v.writeTo(o));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        for (Map.Entry<String, State> stringStateEntry : states.entrySet()) {
            builder.field(stringStateEntry.getKey(), stringStateEntry.getValue());
        }
        return builder;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other instanceof RepositoriesState == false) {
            return false;
        }
        final RepositoriesState that = (RepositoriesState) other;
        return states.equals(that.states);
    }

    @Override
    public int hashCode() {
        return states.hashCode();
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public static final class State implements Writeable, ToXContent {

        private final long generation;

        private final long pendingGeneration;

        private State(long generation, long pendingGeneration) {
            assert generation <= pendingGeneration :
                "Pending generation [" + pendingGeneration + "] smaller than generation [" + generation + "]";
            this.generation = generation;
            this.pendingGeneration = pendingGeneration;
        }

        private State(StreamInput in) throws IOException {
            this(in.readLong(), in.readLong());
        }

        /**
         * Returns the current repository generation for the repository.
         *
         * @return current repository generation that should be used for reads of the repository's {@link RepositoryData}
         */
        public long generation() {
            return generation;
        }

        /**
         * Latest repository generation that a write was attempted for.
         *
         * @return latest repository generation that a write was attempted for
         */
        public long pendingGeneration() {
            return pendingGeneration;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(generation);
            out.writeLong(pendingGeneration);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().field("generation", generation).field("pending", pendingGeneration).endObject();
        }

        @Override
        public boolean isFragment() {
            return false;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other instanceof RepositoriesState.State == false) {
                return false;
            }
            final RepositoriesState.State that = (RepositoriesState.State) other;
            return this.generation == that.generation && this.pendingGeneration == that.pendingGeneration;
        }

        @Override
        public int hashCode() {
            return Objects.hash(generation, pendingGeneration);
        }
    }

    public static final class Builder {

        private final Map<String, State> stateMap = new HashMap<>();

        private Builder() {
        }

        public Builder putAll(RepositoriesState state) {
            stateMap.putAll(state.states);
            return this;
        }

        public Builder putState(String name, long generation, long pending) {
            stateMap.put(name, new State(generation, pending));
            return this;
        }

        public RepositoriesState build() {
            return new RepositoriesState(Map.copyOf(stateMap));
        }
    }
}
