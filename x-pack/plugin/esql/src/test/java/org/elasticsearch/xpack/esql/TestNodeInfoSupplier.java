/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.xpack.esql.planner.NodeInfoSupplier;

public class TestNodeInfoSupplier implements NodeInfoSupplier {

    public static final NodeInfoSupplier INSTANCE = new TestNodeInfoSupplier();

    @Override
    public String clusterName() {
        return "test-cluster";
    }

    @Override
    public String nodeName() {
        return "test-node";
    }
}
