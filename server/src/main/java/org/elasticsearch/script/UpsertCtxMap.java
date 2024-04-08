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
 * Metadata for insert via upsert in the Update context
 */
public class UpsertCtxMap extends UpdateCtxMap {
    public UpsertCtxMap(String index, String id, String op, long now, Map<String, Object> source) {
        super(source, new UpsertMetadata(index, id, op, now));
    }
}
