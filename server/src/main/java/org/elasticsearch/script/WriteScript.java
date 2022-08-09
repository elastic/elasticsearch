/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import java.util.Map;

/**
 * Abstract base class for scripts that write documents.
 * These scripts provide {@code ctx} for backwards compatibility and expose {@link Metadata}.
 */
public abstract class WriteScript {
    protected final CtxMap<?> ctxMap;

    public WriteScript(CtxMap<?> ctxMap) {
        this.ctxMap = ctxMap;
    }

    /** Provides backwards compatibility access to ctx */
    public Map<String, Object> getCtx() {
        return ctxMap;
    }

    /** Return the metadata for this script */
    public Metadata metadata() {
        return ctxMap.getMetadata();
    }
}
