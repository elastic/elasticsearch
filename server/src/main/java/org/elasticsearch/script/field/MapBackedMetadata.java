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
    public static final String OP = "op";
    public static final String SOURCE = "_source";

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

    protected Object getRawVersion() {
        return map.get(VERSION);
    }

    public MapBackedMetadata setVersion(Long version) {
        return set(VERSION, version);
    }

    public String getRouting() {
        return getString(ROUTING);
    }

    public MapBackedMetadata setRouting(String routing) {
        return set(ROUTING, routing);
    }

    public RawOp getOp() {
        String opStr = getString(OP);
        return new RawOp(opStr);
    }

    public MapBackedMetadata setOp(Op op) {
        if (op == null) {
            throw new IllegalArgumentException("operation must be non-null");
        }
        return set(OP, op.name);
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

    /**
     * The Op and underlying String for error messages.
     * str may be null. op is always non-null.
     */
    public static class RawOp {
        public final Op op;
        public final String str;

        public RawOp(String str) {
            this.str = str;
            this.op = Op.fromString(str);
        }
    }
}
