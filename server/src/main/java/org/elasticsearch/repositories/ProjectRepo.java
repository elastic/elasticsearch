/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.repositories;

import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

/**
 * A project qualified repository
 *
 * @param projectId The project that the repository belongs to
 * @param name      Name of the repository
 */
public record ProjectRepo(ProjectId projectId, String name) implements Writeable {

    public static final DiffableUtils.KeySerializer<ProjectRepo> PROJECT_REPO_SERIALIZER = new DiffableUtils.KeySerializer<>() {
        @Override
        public void writeKey(ProjectRepo key, StreamOutput out) throws IOException {
            key.writeTo(out);
        }

        @Override
        public ProjectRepo readKey(StreamInput in) throws IOException {
            return new ProjectRepo(in);
        }
    };

    public ProjectRepo(StreamInput in) throws IOException {
        this(ProjectId.readFrom(in), in.readString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        projectId.writeTo(out);
        out.writeString(name);
    }

    @Override
    public String toString() {
        return projectRepoString(projectId, name);
    }

    public static String projectRepoString(ProjectId projectId, String repositoryName) {
        return "[" + projectId + "][" + repositoryName + "]";
    }
}
