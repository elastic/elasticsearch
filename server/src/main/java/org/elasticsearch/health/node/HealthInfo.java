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
 */
public record HealthInfo(Map<String, DiskHealthInfo> diskInfoByNode, @Nullable DataStreamLifecycleHealthInfo dslHealthInfo)
    implements
        Writeable {

    public static final HealthInfo EMPTY_HEALTH_INFO = new HealthInfo(Map.of(), NO_DSL_ERRORS);

    public HealthInfo(StreamInput input) throws IOException {
        this(
            input.readMap(DiskHealthInfo::new),
            input.getTransportVersion().onOrAfter(TransportVersions.HEALTH_INFO_ENRICHED_WITH_DSL_STATUS)
                ? input.readOptionalWriteable(DataStreamLifecycleHealthInfo::new)
                : null
        );
    }

    @Override
    public void writeTo(StreamOutput output) throws IOException {
        output.writeMap(diskInfoByNode, StreamOutput::writeWriteable);
        if (output.getTransportVersion().onOrAfter(TransportVersions.HEALTH_INFO_ENRICHED_WITH_DSL_STATUS)) {
            output.writeOptionalWriteable(dslHealthInfo);
        }
    }
}
