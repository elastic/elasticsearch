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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;

/**
 * Metadata about registered repository
 */
public class RepositoryMetaData {
    private final String name;
    private final String type;
    private final Settings settings;

    /**
     * Constructs new repository metadata
     *
     * @param name     repository name
     * @param type     repository type
     * @param settings repository settings
     */
    public RepositoryMetaData(String name, String type, Settings settings) {
        this.name = name;
        this.type = type;
        this.settings = settings;
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


    public RepositoryMetaData(StreamInput in) throws IOException {
        name = in.readString();
        type = in.readString();
        settings = Settings.readSettingsFromStream(in);
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
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RepositoryMetaData that = (RepositoryMetaData) o;

        if (!name.equals(that.name)) return false;
        if (!type.equals(that.type)) return false;
        return settings.equals(that.settings);

    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + type.hashCode();
        result = 31 * result + settings.hashCode();
        return result;
    }
}
