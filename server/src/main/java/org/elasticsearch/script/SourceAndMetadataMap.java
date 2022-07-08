/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SourceAndMetadataMap implements MapWrappable {
    protected Map<String, Object> source;
    protected MapWrappable metadata;

    public SourceAndMetadataMap(Map<String, Object> source, MapWrappable metadata) {
        this.source = source;
        this.metadata = metadata;
    }

    @Override
    public boolean isAuthoritative(String key) {
        return true;
    }

    @Override
    public Object put(String key, Object value) {
        if (metadata.isAuthoritative(key)) {
            return metadata.put(key, value);
        }
        return source.put(key, value);
    }

    @Override
    public boolean containsKey(String key) {
        return source.containsKey(key) || metadata.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return source.containsValue(value) || metadata.containsValue(value);
    }

    @Override
    public Object get(String key) {
        if (metadata.isAuthoritative(key)) {
            return metadata.get(key);
        }
        return source.get(key);
    }

    @Override
    public Object remove(String key) {
        if (metadata.isAuthoritative(key)) {
            return metadata.remove(key);
        }
        return source.remove(key);
    }

    @Override
    public List<String> keys() {
        List<String> keys = new ArrayList<>(size());
        keys.addAll(metadata.keys());
        keys.addAll(source.keySet());
        return keys;
    }

    @Override
    public int size() {
        return metadata.size() + source.size();
    }

    @Override
    public MapWrappable clone() {
        return new SourceAndMetadataMap(
            new HashMap<>(source),
            metadata.clone()
        );
    }
}
