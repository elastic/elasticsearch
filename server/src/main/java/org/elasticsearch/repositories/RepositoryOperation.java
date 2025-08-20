/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.repositories;

import org.elasticsearch.cluster.metadata.ProjectId;

/**
 * Coordinates of an operation that modifies a repository, assuming that repository at a specific generation.
 */
public interface RepositoryOperation {

    /**
     * Project for which repository belongs to.
     */
    ProjectId projectId();

    /**
     * Name of the repository affected.
     */
    String repository();

    /**
     * The repository state id at the time the operation began.
     */
    long repositoryStateId();

}
