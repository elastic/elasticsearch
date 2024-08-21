/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.inference;

import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.xcontent.ToXContentObject;

public abstract class ChunkingSettings implements ToXContentObject, VersionedNamedWriteable {
    protected String chunkingStrategy;

    public ChunkingSettings(String chunkingStrategy) {
        this.chunkingStrategy = chunkingStrategy;
    }

    public String getChunkingStrategy() {
        return chunkingStrategy;
    }
}
