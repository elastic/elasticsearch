/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.extras;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.Set;

/**
 * Features for the mapper-extras module.
 */
public class MapperExtrasFeatures implements FeatureSpecification {

    /**
     * Fix for ClassCastException when a {@code search_as_you_type} field uses a {@code search_analyzer}
     * backed by a reloadable token filter (e.g. {@code synonyms_set}).
     */
    public static final NodeFeature SEARCH_AS_YOU_TYPE_RELOADABLE_ANALYZER_FIX = new NodeFeature(
        "mapper.search_as_you_type.reloadable_analyzer_fix"
    );

    @Override
    public Set<NodeFeature> getTestFeatures() {
        return Set.of(SEARCH_AS_YOU_TYPE_RELOADABLE_ANALYZER_FIX);
    }
}
