/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.encrypted.keyrotation;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.repositories.RepositoryOperation;

import java.io.IOException;
import java.util.List;

public final class KeyRotationsInProgress extends AbstractNamedDiffable<ClusterState.Custom> implements ClusterState.Custom {

    public static final KeyRotationsInProgress EMPTY = new KeyRotationsInProgress(List.of());

    public static final String TYPE = "snapshots";

    private final List<Entry> entries;

    private KeyRotationsInProgress(List<Entry> entries) {
        this.entries = List.copyOf(entries);
    }

    public static KeyRotationsInProgress of(List<Entry> entries) {
        return new KeyRotationsInProgress(entries);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return null;
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.CURRENT.minimumCompatibilityVersion();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(entries);
    }

    public static Entry waitingEntry(String repository, long repositoryStateId, String target, String source) {
        return new Entry(target, repository, repositoryStateId, source);
    }

    public static final class Entry implements Writeable, ToXContent, RepositoryOperation {

        private final String target;

        private final String source;

        private final String repo;

        private final long repositoryStateId;

        private final State state;

        private Entry(String target, String repo, long repositoryStateId, String source) {
            this.target = target;
            this.repo = repo;
            this.repositoryStateId = repositoryStateId;
            this.state = State.WAITING;
            this.source = source;
        }

        private Entry(String target, String repo, long repositoryStateId, State state, String source) {
            this.target = target;
            this.repo = repo;
            this.repositoryStateId = repositoryStateId;
            this.state = state;
            this.source = source;
        }

        /**
         * @return target key identifier to rotate to
         */
        public String target() {
            return target;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {

        }

        public Entry asStarted() {
            if (state != State.WAITING) {
                assert false : "[" + this + "] is not waiting";
                throw new IllegalStateException("can't start [" + this + "], is not waiting");
            }
            return new Entry(target, repo, repositoryStateId, State.STARTED, source);
        }

        public Entry asCleanup() {
            if (state != State.STARTED) {
                assert false : "[" + this + "] is not started";
                throw new IllegalStateException("can't cleanup [" + this + "], is not started");
            }
            return new Entry(target, repo, repositoryStateId, State.CLEANUP, source);
        }

        @Override
        public String repository() {
            return repo;
        }

        @Override
        public long repositoryStateId() {
            return repositoryStateId;
        }
    }

    public enum State implements Writeable {

        /**
         * Rotation is waiting to execute because there are repository operations to complete before this rotation may run.
         */
        WAITING((byte) 0),

        /**
         * Rotation is physically executing on the repository.
         */
        STARTED((byte) 1),

        /**
         * Rotation is removing previous key from the repository.
         */
        CLEANUP((byte) 2);

        private final byte value;

        State(byte value) {
            this.value = value;
        }

        public static State readFrom(StreamInput in) throws IOException {
            final byte value = in.readByte();
            switch (value) {
                case 0:
                    return WAITING;
                case 1:
                    return STARTED;
                case 2:
                    return CLEANUP;
                default:
                    throw new IllegalArgumentException("No key rotation state for value [" + value + "]");
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByte(value);
        }
    }
}
