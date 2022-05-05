/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import java.time.ZonedDateTime;

/** Metadata about an ingestion operation, implemented by scripting contexts */
public abstract class Metadata {
    protected final String indexKey;
    protected final String idKey;
    protected final String routingKey;
    protected final String versionKey;
    protected final String versionTypeKey;
    protected final String opKey;

    public Metadata(String indexKey, String idKey, String routingKey, String versionKey, String versionTypeKey, String opKey) {
        this.indexKey = indexKey;
        this.idKey = idKey;
        this.routingKey = routingKey;
        this.versionKey = versionKey;
        this.versionTypeKey = versionTypeKey;
        this.opKey = opKey;
    }

    public String getIndex() {
        return getString(indexKey);
    }

    public void setIndex(String index) {
        put(indexKey, index);
    }

    public String getId() {
        return getString(idKey);
    }

    public void setId(String id) {
        put(idKey, id);
    }

    public String getRouting() {
        return getString(routingKey);
    }

    public void setRouting(String routing) {
        put(routingKey, routing);
    }

    public long getVersion() {
        return getNumber(versionKey).longValue();
    }

    public void setVersion(long version) {
        put(versionKey, version);
    }

    public String getVersionType() {
        return getString(versionTypeKey);
    }

    public abstract void setVersionType(Object versionType);

    public abstract String getOp();

    public abstract void setOp(Object op);

    public abstract ZonedDateTime getTimestamp();

    protected void ensureKeyNotNull(String key) {
        if (key == null) {
            throw new UnsupportedOperationException("not supported in this context");
        }
    }

    protected abstract void put(String key, Object value);

    protected abstract Object get(String key);

    protected String getString(String key) {
        Object obj = get(key);
        if (obj != null) {
            return obj.toString();
        }
        return "";
    }

    protected Number getNumber(String key) {
        Object obj = get(key);
        if (obj instanceof Number number) {
            return number;
        }
        return -1;
    }
}
