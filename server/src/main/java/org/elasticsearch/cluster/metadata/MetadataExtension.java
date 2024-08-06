/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.NamedDiffable;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.EnumSet;

/**
 * Custom metadata that persists (via XContent) across restarts. The deserialization method for each implementation must be registered
 * with the {@link NamedXContentRegistry}.
 */
public interface MetadataExtension extends NamedDiffable<MetadataExtension>, ChunkedToXContent {

    EnumSet<Metadata.XContentContext> context();

    /**
     * @return true if this custom could be restored from snapshot
     */
    default boolean isRestorable() {
        return context().contains(Metadata.XContentContext.SNAPSHOT);
    }
}
