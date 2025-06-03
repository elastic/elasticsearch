/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;

import java.util.Set;

public final class SearchFeatures implements FeatureSpecification {
    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of(KnnVectorQueryBuilder.K_PARAM_SUPPORTED);
    }

    public static final NodeFeature RETRIEVER_RESCORER_ENABLED = new NodeFeature("search.retriever.rescorer.enabled");
    public static final NodeFeature COMPLETION_FIELD_SUPPORTS_DUPLICATE_SUGGESTIONS = new NodeFeature(
        "search.completion_field.duplicate.support"
    );
    public static final NodeFeature INT_SORT_FOR_INT_SHORT_BYTE_FIELDS = new NodeFeature("search.sort.int_sort_for_int_short_byte_fields");

    @Override
    public Set<NodeFeature> getTestFeatures() {
        return Set.of(RETRIEVER_RESCORER_ENABLED, COMPLETION_FIELD_SUPPORTS_DUPLICATE_SUGGESTIONS, INT_SORT_FOR_INT_SHORT_BYTE_FIELDS);
    }
}
