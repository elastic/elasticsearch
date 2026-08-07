/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.Arrays;

/**
 * Shallow snapshot of coordinator search source and indices for populating the search profile {@code profile.request} object when the
 * search request has {@code profile: true}, taken before CCS mutates {@link SearchSourceBuilder#from(int)} /
 * {@link SearchSourceBuilder#size(int)} for sub-requests.
 */
record SearchCoordinatorContext(@Nullable SearchSourceBuilder originalSource, @Nullable String[] requestIndices) {
    static SearchCoordinatorContext none() {
        return new SearchCoordinatorContext(null, null);
    }

    static SearchCoordinatorContext snapshotProfileCoordinatorMetadata(SearchRequest searchRequest) {
        SearchSourceBuilder source = searchRequest.source();
        if (source == null || source.profile() == false) {
            return none();
        }
        return new SearchCoordinatorContext(
            SearchSourceBuilder.shallowCopyForSearchCoordinatorContext(source),
            searchRequest.indices() == null ? null : Arrays.copyOf(searchRequest.indices(), searchRequest.indices().length)
        );
    }
}
