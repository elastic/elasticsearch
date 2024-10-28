/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.version;

import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.indices.SystemIndexDescriptor;

import java.io.IOException;
import java.util.Map;

public class SystemIndexMappingsVersions extends BaseNodeResponse {

    private final Map<String, SystemIndexDescriptor.MappingsVersion> systemIndexMappingsVersions;

    public SystemIndexMappingsVersions(StreamInput in) throws IOException {
        super(in);
        systemIndexMappingsVersions = in.readMap(StreamInput::readString, SystemIndexDescriptor.MappingsVersion::new);
    }

    public SystemIndexMappingsVersions(Map<String, SystemIndexDescriptor.MappingsVersion> systemIndexMappingsVersions, DiscoveryNode node) {
        super(node);
        this.systemIndexMappingsVersions = Map.copyOf(systemIndexMappingsVersions);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeMap(systemIndexMappingsVersions, (o, v) -> v.writeTo(o));
    }
}
