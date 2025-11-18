/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.action.support.nodes.BaseNodesRequest;

import java.util.Arrays;
import java.util.Objects;

public class NodesDeprecationCheckRequest extends BaseNodesRequest {

    public NodesDeprecationCheckRequest(String... nodesIds) {
        super(nodesIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash((Object[]) this.nodesIds());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        NodesDeprecationCheckRequest that = (NodesDeprecationCheckRequest) obj;
        return Arrays.equals(this.nodesIds(), that.nodesIds());
    }
}
