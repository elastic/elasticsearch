/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.internal;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.index.IndexVersion;

import java.util.Collection;

/**
 * Allows plugging in current version elements.
 */
public interface VersionExtension {
    /**
     * Returns additional {@link TransportVersion} defined by extension
     */
    Collection<TransportVersion> getTransportVersions();

    /**
     * Returns the {@link IndexVersion} that Elasticsearch should use.
     * <p>
     * This must be at least as high as the given fallback.
     * @param fallback The latest index version from server
     */
    IndexVersion getCurrentIndexVersion(IndexVersion fallback);
}
