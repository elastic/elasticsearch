/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.project;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.core.FixForMultiProject;

/**
 * This is the {@link ProjectResolver} implementation that stateful uses.
 * It mainly ensures that there's a single and implicit project existing at all times.
 */
public class DefaultProjectResolver implements ProjectResolver {
    public static final DefaultProjectResolver INSTANCE = new DefaultProjectResolver();

    @Override
    @FixForMultiProject
    public ProjectMetadata getProjectMetadata(Metadata metadata) {
        // TODO-multi-project assert no specific project id is requested, and/or that a sole project exists in the cluster state
        return metadata.getProject();
    }

}
