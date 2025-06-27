/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script;

/**
 * A wrapper for a {@link CtxMap} that allows for ad-hoc setting for script execution.
 * This allows for precompilation of scripts that can be executed with different contexts.
 * The wrapped {@link CtxMap} should be cleared after use to avoid leaks.
 */
public class CtxMapWrapper {
    private volatile CtxMap<?> ctxMap;

    public CtxMap<?> getCtxMap() {
        if (ctxMap == null) {
            throw new IllegalStateException("CtxMap is not set");
        }
        return ctxMap;
    }

    public void setCtxMap(CtxMap<?> ctxMap) {
        this.ctxMap = ctxMap;
    }

    public void clearCtxMap() {
        this.ctxMap = null;
    }
}
