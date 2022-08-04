/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.enterprisesearch.action;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.plugins.MapperPlugin;

import java.util.Map;

public class FieldMappingService implements ClusterStateApplier {

    private ClusterState clusterState;

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        clusterState = event.state();
    }

    public Map<String, MappingMetadata> getFieldMapping(String... indices) {
        return clusterState.metadata().findMappings(indices, MapperPlugin.NOOP_FIELD_FILTER, () -> {
        });
    }
}
