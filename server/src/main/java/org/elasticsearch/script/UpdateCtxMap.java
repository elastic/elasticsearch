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
 * Source and metadata for update (as opposed to insert via upsert) in the Update context.
 */
public class UpdateCtxMap extends CtxMap<UpdateMetadata> {

    public UpdateCtxMap(
        String index,
        String id,
        long version,
        String routing,
        String type,
        String op,
        long now,
        Map<String, Object> source
    ) {
        super(source, new UpdateMetadata(index, id, version, routing, type, op, now));
    }

    protected UpdateCtxMap(Map<String, Object> source, UpdateMetadata metadata) {
        super(source, metadata);
    }
}
