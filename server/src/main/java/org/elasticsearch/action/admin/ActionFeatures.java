/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.Set;

import static org.elasticsearch.action.admin.indices.analyze.TransportAnalyzeAction.WRONG_CUSTOM_ANALYZER_RETURN_400;

public class ActionFeatures implements FeatureSpecification {

    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of();
    }


    @Override
    public Set<NodeFeature> getTestFeatures() {
        return Set.of(WRONG_CUSTOM_ANALYZER_RETURN_400);
    }
}
