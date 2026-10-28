/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health.node;

import org.elasticsearch.cluster.metadata.ProjectId;

public record ProjectIndexName(ProjectId projectId, String indexName) implements Comparable<ProjectIndexName> {
    // VisibleForTesting
    public static final String DELIMITER = "/";

    @Override
    public String toString() {
        return toString(true);
    }

    public String toString(boolean withProjectId) {
        if (withProjectId) {
            return projectId.id() + DELIMITER + indexName;
        } else {
            return indexName;
        }
    }

    @Override
    public int compareTo(ProjectIndexName other) {
        return this.toString().compareTo(other.toString());
    }
}
