/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reservedstate;

import org.elasticsearch.cluster.metadata.ProjectId;

/**
 * {@link ReservedStateHandler} for updating project-specific cluster state.
 *
 * @param <T> The type used to represent the state update
 */
public interface ReservedProjectStateHandler<T> extends ReservedStateHandler<T> {

    /**
     * The transformation method implemented by the handler.
     *
     * <p>
     * The transform method of the handler should apply the necessary changes to
     * the cluster state as it normally would in a REST handler. One difference is that the
     * transform method in an reserved state handler must perform all CRUD operations of the cluster
     * state in one go. For that reason, we supply a wrapper class to the cluster state called
     * {@link TransformState}, which contains the current cluster state as well as any previous keys
     * set by this handler on prior invocation.
     *
     * @param projectId The project id for the update state content
     * @param source The parsed information specific to this handler from the combined cluster state content
     * @param prevState The previous cluster state and keys set by this handler (if any)
     * @return The modified state and the current keys set by this handler
     * @throws Exception
     */
    TransformState transform(ProjectId projectId, T source, TransformState prevState) throws Exception;

}
