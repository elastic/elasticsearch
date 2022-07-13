/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.common.util.Maps;

import java.util.Map;

/**
 * Source and metadata for update (as opposed to insert via upsert) in the Update context.
 */
public class UpdateCtxMap extends CtxMap {
    protected static final String SOURCE = "_source";

    public UpdateCtxMap(
        String index,
        String id,
        long version,
        String routing,
        String type,
        String op,
        long timestamp,
        Map<String, Object> source
    ) {
        super(wrapSource(source), new UpdateMetadata(index, id, version, routing, type, op, timestamp));
    }

    protected UpdateCtxMap(Map<String, Object> source, Metadata metadata) {
        super(wrapSource(source), metadata);
    }

    protected static Map<String, Object> wrapSource(Map<String, Object> source) {
        Map<String, Object> wrapper = Maps.newHashMapWithExpectedSize(1);
        wrapper.put(SOURCE, source);
        return wrapper;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Object> getSource() {
        Map<String, Object> wrapped = super.getSource();
        Object rawSource = wrapped.get(SOURCE);
        if (rawSource instanceof Map<?, ?> map) {
            return (Map<String, Object>) map;
        }
        throw new IllegalArgumentException(
            "Expected source to be a map, instead was [" + rawSource + "] with type [" + rawSource.getClass().getCanonicalName() + "]"
        );
    }
}
