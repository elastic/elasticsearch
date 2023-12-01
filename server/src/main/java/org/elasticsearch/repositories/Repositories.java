/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories;

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
}
