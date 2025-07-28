/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.Set;

public class IndexFeatures implements FeatureSpecification {

    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of();
    }

    public static final NodeFeature LOGSDB_NO_HOST_NAME_FIELD = new NodeFeature("index.logsdb_no_host_name_field");

    private static final NodeFeature SYNONYMS_SET_LENIENT_ON_NON_EXISTING = new NodeFeature("index.synonyms_set_lenient_on_non_existing");

    @Override
    public Set<NodeFeature> getTestFeatures() {
        return Set.of(LOGSDB_NO_HOST_NAME_FIELD, SYNONYMS_SET_LENIENT_ON_NON_EXISTING);
    }
}
