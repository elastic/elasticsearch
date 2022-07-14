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

public class BulkCtxMap<T extends BulkMetadata> extends CtxMap {
    protected static final String SOURCE = "_source";

    protected final T metadata;

    /**
     * Create CtxMap from a source and metadata
     *
     * @param source   the source document map
     * @param metadata the metadata map
     */
    public BulkCtxMap(Map<String, Object> source, T metadata) {
        super(wrapSource(source), metadata);
        this.metadata = metadata;
    }

    @Override
    public T getMetadata() {
        return metadata;
    }

    protected static Map<String, Object> wrapSource(Map<String, Object> source) {
        Map<String, Object> wrapper = Maps.newHashMapWithExpectedSize(1);
        wrapper.put(SOURCE, source);
        return wrapper;
    }
}
