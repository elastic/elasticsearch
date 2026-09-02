/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.attachment;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.Set;

public class IngestAttachmentPluginFeatures implements FeatureSpecification {

    static final NodeFeature INGEST_MAX_FIELD_SIZE_FEATURE = new NodeFeature("ingest.attachment.max_field_size_feature");

    @Override
    public Set<NodeFeature> getTestFeatures() {
        return Set.of(INGEST_MAX_FIELD_SIZE_FEATURE);
    }
}
