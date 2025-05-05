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

import java.util.Set;

public final class SearchFeatures implements FeatureSpecification {

    public static final NodeFeature LUCENE_10_0_0_UPGRADE = new NodeFeature("lucene_10_upgrade");
    public static final NodeFeature LUCENE_10_1_0_UPGRADE = new NodeFeature("lucene_10_1_upgrade");

    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of(LUCENE_10_0_0_UPGRADE, LUCENE_10_1_0_UPGRADE);
    }

    public static final NodeFeature RETRIEVER_RESCORER_ENABLED = new NodeFeature("search.retriever.rescorer.enabled");
    public static final NodeFeature COMPLETION_FIELD_SUPPORTS_DUPLICATE_SUGGESTIONS = new NodeFeature(
        "search.completion_field.duplicate.support"
    );
    public static final NodeFeature RESCORER_MISSING_FIELD_BAD_REQUEST = new NodeFeature("search.rescorer.missing.field.bad.request");

    @Override
    public Set<NodeFeature> getTestFeatures() {
        return Set.of(RETRIEVER_RESCORER_ENABLED, COMPLETION_FIELD_SUPPORTS_DUPLICATE_SUGGESTIONS, RESCORER_MISSING_FIELD_BAD_REQUEST);
    }
}
