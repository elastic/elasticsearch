/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health.node;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.reservedstate.service.FileSettingsService.FileSettingsHealthInfo;

import java.io.IOException;
import java.util.Map;

import static java.util.Objects.requireNonNull;
import static org.elasticsearch.health.node.DataStreamLifecycleHealthInfo.NO_DSL_ERRORS;
import static org.elasticsearch.reservedstate.service.FileSettingsService.FileSettingsHealthInfo.INDETERMINATE;

/**
 * This class wraps all the data returned by the health node.
 *
 * @param diskInfoByNode         A Map of node id to DiskHealthInfo for that node
 * @param dslHealthInfo          The data stream lifecycle health information
 * @param repositoriesInfoByNode A Map of node id to RepositoriesHealthInfo for that node
 * @param fileSettingsHealthInfo The file-based settings health information
 */
public record HealthInfo(
    Map<String, DiskHealthInfo> diskInfoByNode,
    @Nullable DataStreamLifecycleHealthInfo dslHealthInfo,
    Map<String, RepositoriesHealthInfo> repositoriesInfoByNode,
    FileSettingsHealthInfo fileSettingsHealthInfo
) implements Writeable {

    public static final HealthInfo EMPTY_HEALTH_INFO = new HealthInfo(Map.of(), NO_DSL_ERRORS, Map.of(), INDETERMINATE);

    public HealthInfo {
        requireNonNull(fileSettingsHealthInfo);
    }

    public HealthInfo(StreamInput input) throws IOException {
        this(
            input.readMap(DiskHealthInfo::new),
            input.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)
                ? input.readOptionalWriteable(DataStreamLifecycleHealthInfo::new)
                : null,
            input.getTransportVersion().onOrAfter(TransportVersions.V_8_13_0) ? input.readMap(RepositoriesHealthInfo::new) : Map.of(),
            includeFileSettings(input.getTransportVersion()) ? input.readOptionalWriteable(FileSettingsHealthInfo::new) : INDETERMINATE
        );
    }

    @Override
    public void writeTo(StreamOutput output) throws IOException {
        output.writeMap(diskInfoByNode, StreamOutput::writeWriteable);
        if (output.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
            output.writeOptionalWriteable(dslHealthInfo);
        }
        if (output.getTransportVersion().onOrAfter(TransportVersions.V_8_13_0)) {
            output.writeMap(repositoriesInfoByNode, StreamOutput::writeWriteable);
        }
        if (includeFileSettings(output.getTransportVersion())) {
            output.writeOptionalWriteable(fileSettingsHealthInfo);
        }
    }

    public static boolean includeFileSettings(TransportVersion transportVersion) {
        return transportVersion.isPatchFrom(TransportVersions.FILE_SETTINGS_HEALTH_INFO_8_19)
            || transportVersion.onOrAfter(TransportVersions.FILE_SETTINGS_HEALTH_INFO);
    }

}
