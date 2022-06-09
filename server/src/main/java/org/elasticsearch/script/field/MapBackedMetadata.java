/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import org.elasticsearch.common.util.Maps;

import java.util.Map;
import java.util.Objects;

/** Metadata storage backed by a Map for compatibility with the ctx Map used by ingest and update scripts. */
public class MapBackedMetadata {
    public static final String INDEX = "_index";
    public static final String ID = "_id";
    public static final String VERSION = "_version";
    public static final String ROUTING = "_routing";
    public static final String SOURCE = "_source";

    /**
     * The backing map has the metadata keys from above.  For backwards compatibility this map is exposed to
     * scripts and they can modify any number of keys.
     *
     * Metadata keys are protected via getters and setters in this class but may be modified externally.
     *
     * Users of the class may add additional metadata keys and helpers by using the {@link #set(String, Object)},
     * {@link #getString(String)} or accessing the underlying map directly.
     */
    protected Map<String, Object> map;

    public MapBackedMetadata(Map<String, Object> map) {
        this.map = map;
    }

    public MapBackedMetadata(int size) {
        this.map = Maps.newMapWithExpectedSize(size);
    }

    public String getIndex() {
        return getString(INDEX);
    }

    public MapBackedMetadata setIndex(String index) {
        return set(INDEX, index);
    }

    public String getId() {
        return getString(ID);
    }

    public MapBackedMetadata setId(String id) {
        return set(ID, id);
    }

    public Long getVersion() {
        Object obj = getRawVersion();
        if (obj == null) {
            return null;
        } else if (obj instanceof Number number) {
            long version = number.longValue();
            if (number.doubleValue() != version) {
                // did we round?
                throw new IllegalArgumentException(
                    "version may only be set to an int or a long but was [" + number + "] with type [" + obj.getClass().getName() + "]"
                );
            }
            return version;
        }
        throw new IllegalArgumentException(
            "version may only be set to an int or a long but was [" + obj + "] with type [" + obj.getClass().getName() + "]"
        );
    }

    public Object getRawVersion() {
        return map.get(VERSION);
    }

    public MapBackedMetadata setVersion(Long version) {
        return set(VERSION, version);
    }

    public void removeVersion() {
        map.remove(VERSION);
    }

    public String getRouting() {
        return getString(ROUTING);
    }

    public MapBackedMetadata setRouting(String routing) {
        return set(ROUTING, routing);
    }

    public String getString(String key) {
        return Objects.toString(map.get(key), null);
    }

    public MapBackedMetadata set(String key, Object value) {
        map.put(key, value);
        return this;
    }

    public Map<String, Object> getMap() {
        return map;
    }

    public MapBackedMetadata setSource(Map<String, Object> source) {
        map.put(SOURCE, source);
        return this;
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> getSource() {
        Object source = map.get(SOURCE);
        if (source instanceof Map<?, ?> map) {
            return (Map<String, Object>) map;
        } else {
            throw new IllegalArgumentException("source should be a map, not [" + source + "] with [" + source.getClass().getName() + "]");
        }
    }
}
