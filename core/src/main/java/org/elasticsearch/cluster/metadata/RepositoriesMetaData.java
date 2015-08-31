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

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.metadata.MetaData.Custom;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.loader.SettingsLoader;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

/**
 * Contains metadata about registered snapshot repositories
 */
public class RepositoriesMetaData extends AbstractDiffable<Custom> implements MetaData.Custom {

    public static final String TYPE = "repositories";

    public static final RepositoriesMetaData PROTO = new RepositoriesMetaData();

    private final List<RepositoryMetaData> repositories;

    /**
     * Constructs new repository metadata
     *
     * @param repositories list of repositories
     */
    public RepositoriesMetaData(RepositoryMetaData... repositories) {
        this.repositories = Arrays.asList(repositories);
    }

    /**
     * Returns list of currently registered repositories
     *
     * @return list of repositories
     */
    public List<RepositoryMetaData> repositories() {
        return this.repositories;
    }

    /**
     * Returns a repository with a given name or null if such repository doesn't exist
     *
     * @param name name of repository
     * @return repository metadata
     */
    public RepositoryMetaData repository(String name) {
        for (RepositoryMetaData repository : repositories) {
            if (name.equals(repository.name())) {
                return repository;
            }
        }
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RepositoriesMetaData that = (RepositoriesMetaData) o;

        return repositories.equals(that.repositories);

    }

    @Override
    public int hashCode() {
        return repositories.hashCode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String type() {
        return TYPE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Custom readFrom(StreamInput in) throws IOException {
        RepositoryMetaData[] repository = new RepositoryMetaData[in.readVInt()];
        for (int i = 0; i < repository.length; i++) {
            repository[i] = RepositoryMetaData.readFrom(in);
        }
        return new RepositoriesMetaData(repository);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(repositories.size());
        for (RepositoryMetaData repository : repositories) {
            repository.writeTo(out);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RepositoriesMetaData fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token;
        List<RepositoryMetaData> repository = new ArrayList<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                String name = parser.currentName();
                if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                    throw new ElasticsearchParseException("failed to parse repository [{}], expected object", name);
                }
                String type = null;
                Settings settings = Settings.EMPTY;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        String currentFieldName = parser.currentName();
                        if ("type".equals(currentFieldName)) {
                            if (parser.nextToken() != XContentParser.Token.VALUE_STRING) {
                                throw new ElasticsearchParseException("failed to parse repository [{}], unknown type", name);
                            }
                            type = parser.text();
                        } else if ("settings".equals(currentFieldName)) {
                            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                                throw new ElasticsearchParseException("failed to parse repository [{}], incompatible params", name);
                            }
                            settings = Settings.settingsBuilder().put(SettingsLoader.Helper.loadNestedFromMap(parser.mapOrdered())).build();
                        } else {
                            throw new ElasticsearchParseException("failed to parse repository [{}], unknown field [{}]", name, currentFieldName);
                        }
                    } else {
                        throw new ElasticsearchParseException("failed to parse repository [{}]", name);
                    }
                }
                if (type == null) {
                    throw new ElasticsearchParseException("failed to parse repository [{}], missing repository type", name);
                }
                repository.add(new RepositoryMetaData(name, type, settings));
            } else {
                throw new ElasticsearchParseException("failed to parse repositories");
            }
        }
        return new RepositoriesMetaData(repository.toArray(new RepositoryMetaData[repository.size()]));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        for (RepositoryMetaData repository : repositories) {
            toXContent(repository, builder, params);
        }
        return builder;
    }

    @Override
    public EnumSet<MetaData.XContentContext> context() {
        return MetaData.API_AND_GATEWAY;
    }

    /**
     * Serializes information about a single repository
     *
     * @param repository repository metadata
     * @param builder    XContent builder
     * @param params     serialization parameters
     * @throws IOException
     */
    public static void toXContent(RepositoryMetaData repository, XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(repository.name(), XContentBuilder.FieldCaseConversion.NONE);
        builder.field("type", repository.type());
        builder.startObject("settings");
        repository.settings().toXContent(builder, params);
        builder.endObject();

        builder.endObject();
    }
}
