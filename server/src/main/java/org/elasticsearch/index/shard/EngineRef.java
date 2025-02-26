/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.shard;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.engine.Engine;

public final class EngineRef extends AbstractRefCounted implements Releasable {

    private final Releasable releasable;
    private final Engine engine;

    public EngineRef(Engine engine, Releasable releasable) {
        this.engine = engine;
        this.releasable = releasable;
    }

    @Nullable
    public Engine getEngineOrNull() {
        return engine;
    }

    public Engine getEngine() {
        var engine = getEngineOrNull();
        if (engine == null) {
            throw new AlreadyClosedException("engine is closed");
        }
        return engine;
    }

    @Override
    protected void closeInternal() {
        releasable.close();
    }

    @Override
    public void close() {
        decRef();
    }
}
