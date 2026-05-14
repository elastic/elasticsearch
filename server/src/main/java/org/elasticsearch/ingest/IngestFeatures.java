/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.Set;

public class IngestFeatures implements FeatureSpecification {
    private static final NodeFeature SIMULATE_INGEST_400_ON_FAILURE = new NodeFeature("simulate.ingest.400_on_failure", true);
    private static final NodeFeature INGEST_APPEND_COPY_FROM = new NodeFeature("ingest.append.copy_from", true);
    private static final NodeFeature INGEST_APPEND_IGNORE_EMPTY_VALUES = new NodeFeature("ingest.append.ignore_empty_values", true);
    private static final NodeFeature RANDOM_SAMPLING = new NodeFeature("random_sampling", true);
    private static final NodeFeature INGEST_APPEND_IGNORE_EMPTY_VALUES_FIX = new NodeFeature("ingest.append.ignore_empty_values_fix", true);

    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of(IngestService.FIELD_ACCESS_PATTERN);
    }

    @Override
    public Set<NodeFeature> getTestFeatures() {
        return Set.of(
            SIMULATE_INGEST_400_ON_FAILURE,
            INGEST_APPEND_COPY_FROM,
            INGEST_APPEND_IGNORE_EMPTY_VALUES,
            RANDOM_SAMPLING,
            INGEST_APPEND_IGNORE_EMPTY_VALUES_FIX
        );
    }
}
