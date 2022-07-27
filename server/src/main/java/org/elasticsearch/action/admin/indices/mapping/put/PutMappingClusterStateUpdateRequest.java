/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.mapping.put;

import org.elasticsearch.cluster.ack.IndicesClusterStateUpdateRequest;
import org.elasticsearch.common.compress.CompressedXContent;

import java.io.IOException;

/**
 * Cluster state update request that allows to put a mapping
 */
public class PutMappingClusterStateUpdateRequest extends IndicesClusterStateUpdateRequest<PutMappingClusterStateUpdateRequest> {

    private final CompressedXContent source;

    public PutMappingClusterStateUpdateRequest(String source) throws IOException {
        this.source = CompressedXContent.fromJSON(source);
    }

    public CompressedXContent source() {
        return source;
    }

}
