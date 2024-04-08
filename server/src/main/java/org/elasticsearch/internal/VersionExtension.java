/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.internal;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.index.IndexVersion;

/**
 * Allows plugging in current version elements.
 */
public interface VersionExtension {
    /**
     * Returns the {@link TransportVersion} that Elasticsearch should use.
     * <p>
     * This must be at least as high as the given fallback.
     * @param fallback The latest transport version from server
     */
    TransportVersion getCurrentTransportVersion(TransportVersion fallback);

    /**
     * Returns the {@link IndexVersion} that Elasticsearch should use.
     * <p>
     * This must be at least as high as the given fallback.
     * @param fallback The latest index version from server
     */
    IndexVersion getCurrentIndexVersion(IndexVersion fallback);
}
