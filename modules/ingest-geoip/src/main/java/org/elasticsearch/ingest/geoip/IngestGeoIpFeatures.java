/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.Set;

import static org.elasticsearch.ingest.geoip.direct.TransportGetDatabaseConfigurationAction.GET_DATABASE_CONFIGURATION_ACTION_MULTI_NODE;

public class IngestGeoIpFeatures implements FeatureSpecification {
    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of(GET_DATABASE_CONFIGURATION_ACTION_MULTI_NODE);
    }
}
