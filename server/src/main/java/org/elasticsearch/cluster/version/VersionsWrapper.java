/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.version;

import org.elasticsearch.TransportVersion;

/**
 * Wraps component version numbers for cluster state
 *
 * <p>Cluster state will need to carry version information for different independently versioned components.
 * This wrapper lets us wrap these versions one level below {@link org.elasticsearch.cluster.ClusterState}.
 * It's similar to {@link org.elasticsearch.cluster.node.VersionInformation}, but this class is meant to
 * be constructed during node startup and hold values from plugins as well.
 *
 * @param transportVersion
 */
public record VersionsWrapper(TransportVersion transportVersion) {}
