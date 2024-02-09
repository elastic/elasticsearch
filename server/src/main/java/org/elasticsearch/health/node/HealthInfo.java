/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.node;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.health.node.DataStreamLifecycleHealthInfo.NO_DSL_ERRORS;

/**
 * This class wraps all the data returned by the health node.
 * @param diskInfoByNode A Map of node id to DiskHealthInfo for that node
 * @param dslHealthInfo The data stream lifecycle health information
 * @param repositoriesInfoByNode A Map of node id to RepositoriesHealthInfo for that node
 */
public record HealthInfo(
    Map<String, DiskHealthInfo> diskInfoByNode,
    @Nullable DataStreamLifecycleHealthInfo dslHealthInfo,
    Map<String, RepositoriesHealthInfo> repositoriesInfoByNode
) implements Writeable {

    public static final HealthInfo EMPTY_HEALTH_INFO = new HealthInfo(Map.of(), NO_DSL_ERRORS, Map.of());

    public HealthInfo(StreamInput input) throws IOException {
        this(
            input.readMap(DiskHealthInfo::new),
            input.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)
                ? input.readOptionalWriteable(DataStreamLifecycleHealthInfo::new)
                : null,
            input.getTransportVersion().onOrAfter(TransportVersions.HEALTH_INFO_ENRICHED_WITH_REPOS)
                ? input.readMap(RepositoriesHealthInfo::new)
                : Map.of()
        );
    }

    @Override
    public void writeTo(StreamOutput output) throws IOException {
        output.writeMap(diskInfoByNode, StreamOutput::writeWriteable);
        if (output.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
            output.writeOptionalWriteable(dslHealthInfo);
        }
        if (output.getTransportVersion().onOrAfter(TransportVersions.HEALTH_INFO_ENRICHED_WITH_REPOS)) {
            output.writeMap(repositoriesInfoByNode, StreamOutput::writeWriteable);
        }
    }
}
