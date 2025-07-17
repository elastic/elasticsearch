/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script;

import org.elasticsearch.script.field.SourceMapField;

import java.util.Map;

/**
 * Abstract base class for scripts that read field values.
 * These scripts provide {@code ctx} for backwards compatibility and expose {@link Metadata}.
 */
public abstract class SourceMapFieldScript {
    protected final CtxMapWrapper ctxMapWrapper;

    public SourceMapFieldScript(CtxMapWrapper ctxMapWrapper) {
        this.ctxMapWrapper = ctxMapWrapper;
    }

    /** Provides backwards compatibility access to ctx */
    public Map<String, Object> getCtx() {
        return ctxMapWrapper.getCtxMap();
    }

    public SourceMapField field(String path) {
        return new SourceMapField(path, ctxMapWrapper::getCtxMap);
    }
}
