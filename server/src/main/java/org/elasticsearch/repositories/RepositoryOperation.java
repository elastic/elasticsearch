/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.repositories;

/**
 * Coordinates of an operation that modifies a repository, assuming that repository at a specific generation.
 */
public interface RepositoryOperation {

    /**
     * Name of the repository affected.
     */
    String repository();

    /**
     * The repository state id at the time the operation began.
     */
    long repositoryStateId();
}
