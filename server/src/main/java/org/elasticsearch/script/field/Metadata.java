/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import java.util.Map;
import java.util.Objects;

/** Metadata about an ingestion operation, implemented by scripting contexts */
public abstract class Metadata {
    public static final String INDEX = "_index";
    public static final String ID = "_id";
    public static final String VERSION = "_version";
    public static final String ROUTING = "_routing";
    public static final String OP = "op";

    protected Map<String, Object> ctx;

    public Metadata(Map<String, Object> ctx) {
        this.ctx = ctx;
    }

    public String getIndex() {
        return getString(INDEX);
    }

    public void setIndex(String index) {
        ctx.put(INDEX, index);
    }

    public String getId() {
        return getString(ID);
    }

    public void setId(String id) {
        ctx.put(ID, id);
    }

    public Long getVersion() {
        Object obj = ctx.get(VERSION);
        if (obj == null) {
            return null;
        } else if (obj instanceof Number number) {
            long version = number.longValue();
            if (number.doubleValue() != version) {
                // did we round?
                throw new IllegalArgumentException("version may only be set to an int or a long but was [" + version + "]");
            }
            return version;
        }
        throw new IllegalArgumentException("unexpected type [" + obj.getClass().getName() + "] for [" + obj + "], expected a Number");
    }

    public void setVersion(Long version) {
        ctx.put(VERSION, version);
    }

    public String getRouting() {
        return getString(ROUTING);
    }

    public void setRouting(String routing) {
        ctx.put(ROUTING, routing);
    }

    protected String getString(String key) {
        return Objects.toString(ctx.get(key), null);
    }
}
