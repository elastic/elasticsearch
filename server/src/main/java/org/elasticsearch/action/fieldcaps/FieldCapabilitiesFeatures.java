/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.Set;

public class FieldCapabilitiesFeatures implements FeatureSpecification {
    public static final NodeFeature FIELD_CAPS_INFERENCE_FIELD = new NodeFeature("field_caps.inference_field");

    @Override
    public Set<NodeFeature> getTestFeatures() {
        return Set.of(FIELD_CAPS_INFERENCE_FIELD);
    }
}
