/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.Set;

import static org.elasticsearch.action.bulk.TransportSimulateBulkAction.SIMULATE_COMPONENT_TEMPLATE_SUBSTITUTIONS;
import static org.elasticsearch.action.bulk.TransportSimulateBulkAction.SIMULATE_MAPPING_VALIDATION;
import static org.elasticsearch.action.bulk.TransportSimulateBulkAction.SIMULATE_MAPPING_VALIDATION_TEMPLATES;

public class BulkFeatures implements FeatureSpecification {
    public Set<NodeFeature> getFeatures() {
        return Set.of(SIMULATE_MAPPING_VALIDATION, SIMULATE_MAPPING_VALIDATION_TEMPLATES, SIMULATE_COMPONENT_TEMPLATE_SUBSTITUTIONS);
    }
}
