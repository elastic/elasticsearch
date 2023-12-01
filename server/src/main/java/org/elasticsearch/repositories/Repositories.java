/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories;

import org.elasticsearch.cluster.metadata.RepositoryMetadata;

import java.util.Map;

public interface Repositories {
    /**
     * @return the current collection of registered repositories, keyed by name.
     */
    Map<String, Repository> getRepositories();

    /**
     * Returns registered repository
     *
     * @param repositoryName repository name
     * @return registered repository
     * @throws RepositoryMissingException if repository with such name isn't registered
     */
    Repository repository(String repositoryName);

    /**
     * Creates a repository holder.
     *
     * <p>WARNING: This method is intended for expert only usage mainly in plugins/modules. Please take note of the following:</p>
     *
     * <ul>
     *     <li>This method does not register the repository (e.g., in the cluster state).</li>
     *     <li>This method starts the repository. The repository should be closed after use.</li>
     *     <li>The repository metadata should be associated to an already registered non-internal repository type and factory pair.</li>
     * </ul>
     *
     * @param repositoryMetadata the repository metadata
     * @return the started repository
     * @throws RepositoryException if repository type is not registered
     */
    Repository createRepository(RepositoryMetadata repositoryMetadata);
}
