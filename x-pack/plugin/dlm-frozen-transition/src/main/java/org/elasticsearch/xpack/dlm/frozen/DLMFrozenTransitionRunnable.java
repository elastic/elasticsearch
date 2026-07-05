/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.dlm.frozen;

import org.elasticsearch.cluster.metadata.ProjectId;

/**
 * A runnable task associated with a specific index transition.
 */
interface DLMFrozenTransitionRunnable extends Runnable {
    String getIndexName();

    ProjectId getProjectId();
}
