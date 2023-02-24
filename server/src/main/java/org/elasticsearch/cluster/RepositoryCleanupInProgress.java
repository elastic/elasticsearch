/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.repositories.RepositoryOperation;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public final class RepositoryCleanupInProgress extends AbstractNamedDiffable<ClusterState.Custom> implements ClusterState.Custom {

    public static final RepositoryCleanupInProgress EMPTY = new RepositoryCleanupInProgress(List.of());

    public static final String TYPE = "repository_cleanup";

    private final List<Entry> entries;

    public RepositoryCleanupInProgress(List<Entry> entries) {
        this.entries = entries;
    }

    RepositoryCleanupInProgress(StreamInput in) throws IOException {
        this.entries = in.readList(Entry::new);
    }

    public static NamedDiff<ClusterState.Custom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(ClusterState.Custom.class, TYPE, in);
    }

    public static Entry startedEntry(String repository, long repositoryStateId) {
        return new Entry(repository, repositoryStateId);
    }

    public boolean hasCleanupInProgress() {
        // TODO: Should we allow parallelism across repositories here maybe?
        return entries.isEmpty() == false;
    }

    public List<Entry> entries() {
        return List.copyOf(entries);
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(entries);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params ignored) {
        return Iterators.concat(
            Iterators.single((builder, params) -> builder.startArray(TYPE)),
            entries.stream().<ToXContent>map(entry -> (builder, params) -> {
                builder.startObject();
                builder.field("repository", entry.repository);
                builder.endObject();
                return builder;
            }).iterator(),
            Iterators.single((builder, params) -> builder.endArray())
        );
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.V_7_4_0;
    }

    public static final class Entry implements Writeable, RepositoryOperation {

        private final String repository;

        private final long repositoryStateId;

        private Entry(StreamInput in) throws IOException {
            repository = in.readString();
            repositoryStateId = in.readLong();
        }

        public Entry(String repository, long repositoryStateId) {
            this.repository = repository;
            this.repositoryStateId = repositoryStateId;
        }

        @Override
        public long repositoryStateId() {
            return repositoryStateId;
        }

        @Override
        public String repository() {
            return repository;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(repository);
            out.writeLong(repositoryStateId);
        }

        @Override
        public String toString() {
            return "{" + repository + '}' + '{' + repositoryStateId + '}';
        }
    }
}
