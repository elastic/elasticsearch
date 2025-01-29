/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action;

import org.elasticsearch.action.search.SearchContextId;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.index.Index;

import java.util.Map;

public class MockResolvedIndices extends ResolvedIndices {
    public MockResolvedIndices(
        Map<String, OriginalIndices> remoteClusterIndices,
        OriginalIndices localIndices,
        Map<Index, IndexMetadata> localIndexMetadata,
        SearchContextId searchContextId
    ) {
        super(remoteClusterIndices, localIndices, localIndexMetadata, searchContextId);
    }

    public MockResolvedIndices(
        Map<String, OriginalIndices> remoteClusterIndices,
        OriginalIndices localIndices,
        Map<Index, IndexMetadata> localIndexMetadata
    ) {
        super(remoteClusterIndices, localIndices, localIndexMetadata);
    }
}
