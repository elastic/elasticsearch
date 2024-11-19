/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.rest.action.admin.cluster.RestNodesCapabilitiesAction;

import java.util.Set;

import static org.elasticsearch.search.fetch.subphase.highlight.DefaultHighlighter.UNIFIED_HIGHLIGHTER_MATCHED_FIELDS;

public class RestFeatures implements FeatureSpecification {
    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of(
            RestNodesCapabilitiesAction.CAPABILITIES_ACTION,
            RestNodesCapabilitiesAction.LOCAL_ONLY_CAPABILITIES,
            UNIFIED_HIGHLIGHTER_MATCHED_FIELDS
        );
    }
}
