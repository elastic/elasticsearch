/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

/**
 * Allow modules to atomically modify ProjectMetadata when an index <code>index.lifecycle.name</code> changes.
 */
public interface IndexNewPolicyProjectMetadataModifier {

    /**
     * Notify that {@link IndexMetadata#LIFECYCLE_NAME} setting changed for an index.
     * Can but doesn't need to modify the provided {@link ProjectMetadata.Builder}.
     * We will have to modify the ProjectMetadata anyway to update the index settings so no need to indicate whether a change was made.
     */
    void potentiallyModify(ProjectMetadata currentProject, ProjectMetadata.Builder builder, String indexName, String newLifecyclePolicy);
}
