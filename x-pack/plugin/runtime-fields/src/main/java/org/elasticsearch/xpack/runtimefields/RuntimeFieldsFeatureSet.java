/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.runtimefields;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.runtimefields.RuntimeFieldsFeatureSetUsage;

import java.util.Map;

public class RuntimeFieldsFeatureSet implements XPackFeatureSet {

    private final ClusterService clusterService;

    @Inject
    public RuntimeFieldsFeatureSet(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public String name() {
        return XPackField.RUNTIME_FIELDS;
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
        listener.onResponse(RuntimeFieldsFeatureSetUsage.fromMetadata(clusterService.state().metadata()));
    }
}
