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

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.RepositoryData;

import java.io.IOException;
import java.util.Objects;

/**
 * Metadata about registered repository
 */
public class RepositoryMetaData {

    public static final Version REPO_GEN_IN_CS_VERSION = Version.V_8_0_0;

    private final String name;
    private final String type;
    private final Settings settings;

    private final long generation;

    private final long pendingGeneration;

    /**
     * Constructs new repository metadata
     *
     * @param name     repository name
     * @param type     repository type
     * @param settings repository settings
     */
    public RepositoryMetaData(String name, String type, Settings settings) {
        this(name, type, settings, RepositoryData.UNKNOWN_REPO_GEN, RepositoryData.UNKNOWN_REPO_GEN);
    }

    private RepositoryMetaData(String name, String type, Settings settings, long generation, long pendingGeneration) {
        this.name = name;
        this.type = type;
        this.settings = settings;
        this.generation = generation;
        this.pendingGeneration = pendingGeneration;
    }

    /**
     * Returns repository name
     *
     * @return repository name
     */
    public String name() {
        return this.name;
    }

    /**
     * Returns repository type
     *
     * @return repository type
     */
    public String type() {
        return this.type;
    }

    /**
     * Returns repository settings
     *
     * @return repository settings
     */
    public Settings settings() {
        return this.settings;
    }

    public long generation() {
        return generation;
    }

    public long pendingGeneration() {
        return pendingGeneration;
    }

    public RepositoryMetaData(StreamInput in) throws IOException {
        name = in.readString();
        type = in.readString();
        settings = Settings.readSettingsFromStream(in);
        if (in.getVersion().onOrAfter(REPO_GEN_IN_CS_VERSION)) {
            generation = in.readLong();
            pendingGeneration = in.readLong();
        } else {
            generation = RepositoryData.UNKNOWN_REPO_GEN;
            pendingGeneration = RepositoryData.UNKNOWN_REPO_GEN;
        }
    }

    /**
     * Writes repository metadata to stream output
     *
     * @param out stream output
     */
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(type);
        Settings.writeSettingsToStream(settings, out);
        if (out.getVersion().onOrAfter(REPO_GEN_IN_CS_VERSION)) {
            out.writeLong(generation);
            out.writeLong(pendingGeneration);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RepositoryMetaData that = (RepositoryMetaData) o;

        if (!name.equals(that.name)) return false;
        if (!type.equals(that.type)) return false;
        if (generation != that.generation) return false;
        if (pendingGeneration != that.pendingGeneration) return false;
        return settings.equals(that.settings);

    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, settings, generation, pendingGeneration);
    }
}
