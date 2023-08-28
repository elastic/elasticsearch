/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.node;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.SystemIndexDescriptor;

import java.util.Map;

/**
 * Holds versioning information for different system components
 *
 * TODO[wrb]: should this go in the cluster package, since it's for use in cluster state?
 */
public record VersionsWrapper(
    TransportVersion transportVersion,
    IndexVersion indexVersion,
    Map<String, SystemIndexDescriptor.MappingsVersion> systemIndexMappingsVersions
) {

    public static VersionsWrapper STATIC_VERSIONS = new VersionsWrapper(TransportVersion.current(), IndexVersion.current(), Map.of());

}
