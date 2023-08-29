/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.node;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.indices.SystemIndexDescriptor;

import java.io.IOException;
import java.util.Map;

/**
 * Holds versioning information for different system components
 *
 * TODO[wrb]: should this go in the cluster package, since it's for use in cluster state?
 */
public record VersionsWrapper(Map<String, SystemIndexDescriptor.MappingsVersion> systemIndexMappingsVersions) {

    public static VersionsWrapper EMPTY = new VersionsWrapper(Map.of());

    public void writeVersion(StreamOutput streamOutput) throws IOException {
        streamOutput.writeMap(this.systemIndexMappingsVersions(), StreamOutput::writeString, (o, v) -> v.writeToStream(o));
    }

    public static VersionsWrapper readVersion(StreamInput streamInput) throws IOException {
        return new VersionsWrapper(streamInput.readMap(StreamInput::readString, SystemIndexDescriptor.MappingsVersion::readValue));
    }
}
