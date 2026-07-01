/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.script.CtxMap;

import java.util.Map;

/**
 * Ingest ctx view for scripts.
 * Exposes ingest metadata as {@code ctx._ingest}.
 */
final class IngestScriptCtxMap extends CtxMap<IngestDocMetadata> {

    private final Map<String, Object> ingestMetadata;

    IngestScriptCtxMap(Map<String, Object> source, IngestDocMetadata metadata, Map<String, Object> ingestMetadata) {
        super(source, metadata);
        this.ingestMetadata = ingestMetadata;
    }

    @Override
    protected boolean directSourceAccess() {
        return true;
    }

    @Override
    public Object get(Object key) {
        if (IngestDocument.INGEST_KEY.equals(key)) {
            return ingestMetadata;
        }
        return super.get(key);
    }

    @Override
    public Object getOrDefault(Object key, Object defaultValue) {
        if (IngestDocument.INGEST_KEY.equals(key)) {
            return ingestMetadata;
        }
        return super.getOrDefault(key, defaultValue);
    }
}
