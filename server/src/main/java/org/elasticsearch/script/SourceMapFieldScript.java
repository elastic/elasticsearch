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
 * Abstract base class for that exposes the non-mutable field APIs to scripts.
 */
public abstract class SourceMapFieldScript {
    protected final Map<String, Object> ctxMap;

    public SourceMapFieldScript(Map<String, Object> ctxMap) {
        this.ctxMap = ctxMap;
    }

    /**
     * Expose the {@link SourceMapField field} API
     *
     * @param path the path to the field in the source map
     * @return a new {@link SourceMapField} instance for the specified path
     */
    public SourceMapField field(String path) {
        return new SourceMapField(path, () -> ctxMap);
    }
}
