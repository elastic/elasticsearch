/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.snapshots.SnapshotsService;

import java.io.IOException;
import java.util.Objects;

/**
 * Metadata about registered repository
 */
public class RepositoryMetadata implements Writeable {

    private final String name;
    private final String uuid;
    private final String type;
    private final Settings settings;

    /**
     * Safe repository generation.
     */
    private final long generation;

    /**
     * Pending repository generation.
     */
    private final long pendingGeneration;

    /**
     * Constructs new repository metadata
     *
     * @param name     repository name
     * @param type     repository type
     * @param settings repository settings
     */
    public RepositoryMetadata(String name, String type, Settings settings) {
        this(name, RepositoryData.MISSING_UUID, type, settings, RepositoryData.UNKNOWN_REPO_GEN, RepositoryData.EMPTY_REPO_GEN);
    }

    public RepositoryMetadata(RepositoryMetadata metadata, long generation, long pendingGeneration) {
        this(metadata.name, metadata.uuid, metadata.type, metadata.settings, generation, pendingGeneration);
    }

    public RepositoryMetadata(String name, String uuid, String type, Settings settings, long generation, long pendingGeneration) {
        this.name = name;
        this.uuid = uuid;
        this.type = type;
        this.settings = settings;
        this.generation = generation;
        this.pendingGeneration = pendingGeneration;
        assert generation <= pendingGeneration
            : "Pending generation [" + pendingGeneration + "] must be greater or equal to generation [" + generation + "]";
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
     * Return the repository UUID, if set and known. The repository UUID is stored in the repository and typically populated here when the
     * repository is registered or when we write to it. It may not be set if the repository is maintaining support for versions before
     * {@link SnapshotsService#UUIDS_IN_REPO_DATA_VERSION}. It may not be known if the repository was registered with {@code
     * ?verify=false} and has had no subsequent writes. Consumers may, if desired, try and fill in a missing value themselves by retrieving
     * the {@link RepositoryData} and calling {@link org.elasticsearch.repositories.RepositoriesService#updateRepositoryUuidInMetadata}.
     *
     * @return repository UUID, or {@link RepositoryData#MISSING_UUID} if the UUID is not set or not known.
     */
    public String uuid() {
        return this.uuid;
    }

    /**
     * Returns repository settings
     *
     * @return repository settings
     */
    public Settings settings() {
        return this.settings;
    }

    /**
     * Returns the safe repository generation. {@link RepositoryData} for this generation is assumed to exist in the repository.
     * All operations on the repository must be based on the {@link RepositoryData} at this generation.
     * See package level documentation for the blob store based repositories {@link org.elasticsearch.repositories.blobstore} for details
     * on how this value is used during snapshots.
     * @return safe repository generation
     */
    public long generation() {
        return generation;
    }

    /**
     * Returns the pending repository generation. {@link RepositoryData} for this generation and all generations down to the safe
     * generation {@link #generation} may exist in the repository and should not be reused for writing new {@link RepositoryData} to the
     * repository.
     * See package level documentation for the blob store based repositories {@link org.elasticsearch.repositories.blobstore} for details
     * on how this value is used during snapshots.
     *
     * @return highest pending repository generation
     */
    public long pendingGeneration() {
        return pendingGeneration;
    }

    public RepositoryMetadata(StreamInput in) throws IOException {
        name = in.readString();
        if (in.getVersion().onOrAfter(SnapshotsService.UUIDS_IN_REPO_DATA_VERSION)) {
            uuid = in.readString();
        } else {
            uuid = RepositoryData.MISSING_UUID;
        }
        type = in.readString();
        settings = Settings.readSettingsFromStream(in);
        generation = in.readLong();
        pendingGeneration = in.readLong();
    }

    /**
     * Writes repository metadata to stream output
     *
     * @param out stream output
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        if (out.getVersion().onOrAfter(SnapshotsService.UUIDS_IN_REPO_DATA_VERSION)) {
            out.writeString(uuid);
        }
        out.writeString(type);
        Settings.writeSettingsToStream(settings, out);
        out.writeLong(generation);
        out.writeLong(pendingGeneration);
    }

    /**
     * Checks if this instance is equal to the other instance in all fields other than {@link #generation} and {@link #pendingGeneration}.
     *
     * @param other other repository metadata
     * @return {@code true} if both instances equal in all fields but the generation fields
     */
    public boolean equalsIgnoreGenerations(RepositoryMetadata other) {
        return name.equals(other.name) && uuid.equals(other.uuid()) && type.equals(other.type()) && settings.equals(other.settings());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RepositoryMetadata that = (RepositoryMetadata) o;

        if (name.equals(that.name) == false) return false;
        if (uuid.equals(that.uuid) == false) return false;
        if (type.equals(that.type) == false) return false;
        if (generation != that.generation) return false;
        if (pendingGeneration != that.pendingGeneration) return false;
        return settings.equals(that.settings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, uuid, type, settings, generation, pendingGeneration);
    }

    @Override
    public String toString() {
        return "RepositoryMetadata{"
            + name
            + "}{"
            + uuid
            + "}{"
            + type
            + "}{"
            + settings
            + "}{"
            + generation
            + "}{"
            + pendingGeneration
            + "}";
    }

    public RepositoryMetadata withUuid(String uuid) {
        return new RepositoryMetadata(name, uuid, type, settings, generation, pendingGeneration);
    }

    public RepositoryMetadata withSettings(Settings settings) {
        return new RepositoryMetadata(name, uuid, type, settings, generation, pendingGeneration);
    }
}
