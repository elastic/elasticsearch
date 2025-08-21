/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.mapping.put;

import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;

import java.io.IOException;
import java.util.Objects;

/**
 * Cluster state update request that allows to put a mapping
 */
public record PutMappingClusterStateUpdateRequest(
    TimeValue masterNodeTimeout,
    TimeValue ackTimeout,
    CompressedXContent source,
    boolean autoUpdate,
    Index[] indices
) {
    public PutMappingClusterStateUpdateRequest {
        Objects.requireNonNull(masterNodeTimeout);
        Objects.requireNonNull(ackTimeout);
        Objects.requireNonNull(source);
        Objects.requireNonNull(indices);
    }

    public PutMappingClusterStateUpdateRequest(
        TimeValue masterNodeTimeout,
        TimeValue ackTimeout,
        String source,
        boolean autoUpdate,
        Index... indices
    ) throws IOException {
        this(masterNodeTimeout, ackTimeout, CompressedXContent.fromJSON(source), autoUpdate, indices);
    }
}
