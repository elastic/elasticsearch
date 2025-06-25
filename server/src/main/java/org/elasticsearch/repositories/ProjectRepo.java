/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
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
