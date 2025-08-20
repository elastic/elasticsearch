/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.Set;

public class IngestFeatures implements FeatureSpecification {
    @Override
    public Set<NodeFeature> getFeatures() {
        if (DataStream.LOGS_STREAM_FEATURE_FLAG) {
            return Set.of(IngestService.FIELD_ACCESS_PATTERN);
        } else {
            return Set.of();
        }
    }
}
