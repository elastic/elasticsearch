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
package org.elasticsearch.cluster;

import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public final class RepositoryCleanupInProgress extends AbstractNamedDiffable<ClusterState.Custom> implements ClusterState.Custom {

    public static final String TYPE = "repository_cleanup";

    private final List<Entry> entries;

    public RepositoryCleanupInProgress(Entry... entries) {
        this.entries = Arrays.asList(entries);
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

    public boolean cleanupInProgress() {
        // TODO: Should we allow parallelism across repositories here maybe?
        return entries.isEmpty();
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
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(TYPE);
        for (Entry entry : entries) {
            builder.startObject();
            {
                builder.field("repository", entry.repository);
            }
            builder.endObject();
        }
        builder.endArray();
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_7_4_0;
    }

    public static final class Entry implements Writeable {

        private final String repository;

        private final long repositoryStateId;

        private Entry(StreamInput in) throws IOException {
            repository = in.readString();
            repositoryStateId = in.readLong();
        }

        private Entry(String repository, long repositoryStateId) {
            this.repository = repository;
            this.repositoryStateId = repositoryStateId;
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
