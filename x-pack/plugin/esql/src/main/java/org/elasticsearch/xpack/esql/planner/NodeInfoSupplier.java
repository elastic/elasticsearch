/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.cluster.service.ClusterService;

public interface NodeInfoSupplier {

    String clusterName();

    String nodeName();

    static NodeInfoSupplier from(ClusterService clusterService) {
        return new NodeInfoSupplier() {
            @Override
            public String clusterName() {
                return clusterService.getClusterName().value();
            }

            @Override
            public String nodeName() {
                return clusterService.getNodeName();
            }
        };
    }
}
