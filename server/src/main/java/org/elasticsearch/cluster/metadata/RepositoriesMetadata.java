/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.function.UnaryOperator;

/**
 * Contains metadata about registered snapshot repositories
 */
public class RepositoriesMetadata extends AbstractNamedDiffable<Metadata.ProjectCustom> implements Metadata.ProjectCustom {

    public static final String TYPE = "repositories";

    public static final RepositoriesMetadata EMPTY = new RepositoriesMetadata(Collections.emptyList());

    /**
     * Serialization parameter used to hide the {@link RepositoryMetadata#generation()} and {@link RepositoryMetadata#pendingGeneration()}
     * in {@link org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesResponse}.
     */
    public static final String HIDE_GENERATIONS_PARAM = "hide_generations";

    private final List<RepositoryMetadata> repositories;

    @Deprecated(forRemoval = true)
    public static RepositoriesMetadata get(ClusterState state) {
        return get(state.metadata().getDefaultProject());
    }

    public static RepositoriesMetadata get(ProjectMetadata project) {
        return project.custom(TYPE, EMPTY);
    }

    /**
     * Constructs new repository metadata
     *
     * @param repositories list of repositories
     */
    public RepositoriesMetadata(List<RepositoryMetadata> repositories) {
        this.repositories = Collections.unmodifiableList(repositories);
    }

    /**
     * Creates a new instance that has the given repository moved to the given {@code safeGeneration} and {@code pendingGeneration}.
     *
     * @param repoName          repository name
     * @param safeGeneration    new safe generation
     * @param pendingGeneration new pending generation
     * @return new instance with updated generations
     */
    public RepositoriesMetadata withUpdatedGeneration(String repoName, long safeGeneration, long pendingGeneration) {
        return withUpdate(repoName, repositoryMetadata -> new RepositoryMetadata(repositoryMetadata, safeGeneration, pendingGeneration));
    }

    /**
     * Creates a new instance that records the UUID of the given repository.
     *
     * @param repoName          repository name
     * @param uuid              repository uuid
     * @return new instance with updated uuid
     */
    public RepositoriesMetadata withUuid(String repoName, String uuid) {
        return withUpdate(repoName, repositoryMetadata -> repositoryMetadata.withUuid(uuid));
    }

    private RepositoriesMetadata withUpdate(String repoName, UnaryOperator<RepositoryMetadata> update) {
        int indexOfRepo = -1;
        for (int i = 0; i < repositories.size(); i++) {
            if (repositories.get(i).name().equals(repoName)) {
                indexOfRepo = i;
                break;
            }
        }
        if (indexOfRepo < 0) {
            throw new IllegalArgumentException("Unknown repository [" + repoName + "]");
        }
        final List<RepositoryMetadata> updatedRepos = new ArrayList<>(repositories);
        updatedRepos.set(indexOfRepo, update.apply(repositories.get(indexOfRepo)));
        return new RepositoriesMetadata(updatedRepos);
    }

    /**
     * Returns list of currently registered repositories
     *
     * @return list of repositories
     */
    public List<RepositoryMetadata> repositories() {
        return this.repositories;
    }

    /**
     * Returns a repository with a given name or null if such repository doesn't exist
     *
     * @param name name of repository
     * @return repository metadata
     */
    public RepositoryMetadata repository(String name) {
        for (RepositoryMetadata repository : repositories) {
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

        RepositoriesMetadata that = (RepositoriesMetadata) o;

        return repositories.equals(that.repositories);
    }

    /**
     * Checks if this instance and the given instance share the same repositories by checking that this instances' repositories and the
     * repositories in {@code other} are equal or only differ in their values of {@link RepositoryMetadata#generation()} and
     * {@link RepositoryMetadata#pendingGeneration()}.
     *
     * @param other other repositories metadata
     * @return {@code true} iff both instances contain the same repositories apart from differences in generations
     */
    public boolean equalsIgnoreGenerations(@Nullable RepositoriesMetadata other) {
        if (other == null) {
            return false;
        }
        if (other.repositories.size() != repositories.size()) {
            return false;
        }
        for (int i = 0; i < repositories.size(); i++) {
            if (repositories.get(i).equalsIgnoreGenerations(other.repositories.get(i)) == false) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        return repositories.hashCode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.MINIMUM_COMPATIBLE;
    }

    public RepositoriesMetadata(StreamInput in) throws IOException {
        this.repositories = in.readCollectionAsImmutableList(RepositoryMetadata::new);
    }

    public static NamedDiff<Metadata.ProjectCustom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(Metadata.ProjectCustom.class, TYPE, in);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(repositories);
    }

    public static RepositoriesMetadata fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token;
        List<RepositoryMetadata> repository = new ArrayList<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                String name = parser.currentName();
                if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                    throw new ElasticsearchParseException("failed to parse repository [{}], expected object", name);
                }
                String uuid = RepositoryData.MISSING_UUID;
                String type = null;
                Settings settings = Settings.EMPTY;
                long generation = RepositoryData.UNKNOWN_REPO_GEN;
                long pendingGeneration = RepositoryData.EMPTY_REPO_GEN;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        String currentFieldName = parser.currentName();
                        if ("uuid".equals(currentFieldName)) {
                            if (parser.nextToken() != XContentParser.Token.VALUE_STRING) {
                                throw new ElasticsearchParseException("failed to parse repository [{}], uuid not a string", name);
                            }
                            uuid = parser.text();
                        } else if ("type".equals(currentFieldName)) {
                            if (parser.nextToken() != XContentParser.Token.VALUE_STRING) {
                                throw new ElasticsearchParseException("failed to parse repository [{}], unknown type", name);
                            }
                            type = parser.text();
                        } else if ("settings".equals(currentFieldName)) {
                            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                                throw new ElasticsearchParseException("failed to parse repository [{}], incompatible params", name);
                            }
                            settings = Settings.fromXContent(parser);
                        } else if ("generation".equals(currentFieldName)) {
                            if (parser.nextToken() != XContentParser.Token.VALUE_NUMBER) {
                                throw new ElasticsearchParseException("failed to parse repository [{}], unknown type", name);
                            }
                            generation = parser.longValue();
                        } else if ("pending_generation".equals(currentFieldName)) {
                            if (parser.nextToken() != XContentParser.Token.VALUE_NUMBER) {
                                throw new ElasticsearchParseException("failed to parse repository [{}], unknown type", name);
                            }
                            pendingGeneration = parser.longValue();
                        } else {
                            throw new ElasticsearchParseException(
                                "failed to parse repository [{}], unknown field [{}]",
                                name,
                                currentFieldName
                            );
                        }
                    } else {
                        throw new ElasticsearchParseException("failed to parse repository [{}]", name);
                    }
                }
                if (type == null) {
                    throw new ElasticsearchParseException("failed to parse repository [{}], missing repository type", name);
                }
                repository.add(new RepositoryMetadata(name, uuid, type, settings, generation, pendingGeneration));
            } else {
                throw new ElasticsearchParseException("failed to parse repositories");
            }
        }
        return new RepositoriesMetadata(repository);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params ignored) {
        return Iterators.map(repositories.iterator(), repository -> (builder, params) -> toXContent(repository, builder, params));
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return Metadata.API_AND_GATEWAY;
    }

    /**
     * Serializes information about a single repository
     *
     * @param repository repository metadata
     * @param builder    XContent builder
     * @param params     serialization parameters
     */
    public static XContentBuilder toXContent(RepositoryMetadata repository, XContentBuilder builder, ToXContent.Params params)
        throws IOException {
        builder.startObject(repository.name());
        builder.field("type", repository.type());
        if (repository.uuid().equals(RepositoryData.MISSING_UUID) == false) {
            builder.field("uuid", repository.uuid());
        }
        builder.startObject("settings");
        repository.settings().toXContent(builder, params);
        builder.endObject();

        if (params.paramAsBoolean(HIDE_GENERATIONS_PARAM, false) == false) {
            builder.field("generation", repository.generation());
            builder.field("pending_generation", repository.pendingGeneration());
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
