/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.datastreams;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.datastreams.DataStreamFeatureSetUsage;

import java.util.Map;

public class DataStreamFeatureSet implements XPackFeatureSet {

    private final ClusterService clusterService;

    @Inject
    public DataStreamFeatureSet(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public String name() {
        return XPackField.DATA_STREAMS;
    }

    @Override
    public boolean available() {
        return true;
    }

    @Override
    public boolean enabled() {
        return true;
    }

    @Override
    public Map<String, Object> nativeCodeInfo() {
        return null;
    }

    @Override
    public void usage(ActionListener<Usage> listener) {
        final ClusterState state = clusterService.state();
        final Map<String, DataStream> dataStreams = state.metadata().dataStreams();
        final DataStreamFeatureSetUsage.DataStreamStats stats = new DataStreamFeatureSetUsage.DataStreamStats(
            dataStreams.size(),
            dataStreams.values().stream().map(ds -> ds.getIndices().size()).reduce(Integer::sum).orElse(0)
        );
        final DataStreamFeatureSetUsage usage = new DataStreamFeatureSetUsage(stats);
        listener.onResponse(usage);
    }

}
