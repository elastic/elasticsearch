/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import org.elasticsearch.index.VersionType;

import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/** Metadata about an ingestion operation, implemented by scripting contexts */
public abstract class Metadata {
    protected static final String INDEX = "index";
    protected static final String ID = "id";
    protected static final String VERSION = "version";
    protected static final String ROUTING = "routing";
    protected static final String OP = "op";
    protected static final String VERSION_TYPE = "version type";
    protected static final String TIMESTAMP = "timestamp";

    protected final EnumSet<Op> VALID_OPS;

    protected final String indexKey;
    protected final String idKey;
    protected final String routingKey;
    protected final String versionKey;
    protected final String versionTypeKey;
    protected final String opKey;

    protected final Map<String, Object> ctx;

    public Metadata(
        Map<String, Object> ctx,
        String indexKey,
        String idKey,
        String routingKey,
        String versionKey,
        String versionTypeKey,
        String opKey,
        EnumSet<Op> validOps
    ) {
        this.ctx = Objects.requireNonNull(ctx);
        this.indexKey = indexKey;
        this.idKey = idKey;
        this.routingKey = routingKey;
        this.versionKey = versionKey;
        this.versionTypeKey = versionTypeKey;
        this.opKey = opKey;
        this.VALID_OPS = validOps;
    }

    public String getIndex() {
        if (indexKey == null) {
            unsupported(INDEX, null);
        }
        return getString(indexKey);
    }

    public void setIndex(String index) {
        if (indexKey == null) {
            unsupported(INDEX, null);
        }
        put(indexKey, index);
    }

    public String getId() {
        if (idKey == null) {
            unsupported(ID, null);
        }
        return getString(idKey);
    }

    public void setId(String id) {
        if (idKey == null) {
            unsupported(ID, null);
        }
        put(idKey, id);
    }

    public String getRouting() {
        if (routingKey == null) {
            unsupported(ROUTING, null);
        }
        return getString(routingKey);
    }

    public void setRouting(String routing) {
        if (routingKey == null) {
            unsupported(ROUTING, null);
        }
        put(routingKey, routing);
    }

    public Long getVersion() {
        if (versionKey == null) {
            unsupported(VERSION, null);
        }
        return getNumber(versionKey).longValue();
    }

    public void setVersion(Long version) {
        if (versionKey == null) {
            unsupported(VERSION, null);
        }
        put(versionKey, version);
    }

    public VersionType getVersionType() {
        if (versionTypeKey == null) {
            unsupported(VERSION_TYPE, null);
        }
        String str = getString(versionTypeKey);
        if (str == null) {
            return null;
        }
        return VersionType.fromString(str.toLowerCase(Locale.ROOT));
    }

    public void setVersionType(VersionType versionType) {
        if (versionTypeKey == null) {
            unsupported(VERSION_TYPE, null);
        }
        if (versionType == null) {
            put(versionTypeKey, null);
        } else {
            put(versionTypeKey, VersionType.toString(versionType));
        }
    }

    public Op getOp() {
        return objectToOp(get(opKey));
    }

    public void setOp(Op op) {
        validateOp(op);
        // to string keeps compatibility with existing string APIs
        put(opKey, objectToOp(op).toString());
    }

    protected void validateOp(Op op) {
        if (op != null && VALID_OPS != null && VALID_OPS.contains(op) == false) {
            throw new IllegalArgumentException(
                "Operation type [" + op.name + "] not allowed, only " + Arrays.toString(VALID_OPS.toArray()) + " are allowed"
            );
        }
    }

    protected Op objectToOp(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof Op op) {
            return op;
        }
        if (obj instanceof String str) {
            return Op.fromString(str);
        }
        throw new IllegalArgumentException("Invalid type [" + obj.getClass().getName() + "] for Op [" + obj + "], expected String or Op");
    }

    public ZonedDateTime getTimestamp() {
        unsupported(TIMESTAMP, null);
        return null;
    }

    protected void put(String key, Object value) {
        ctx.put(key, value);
    }

    protected Object get(String key) {
        return ctx.get(key);
    }

    protected String getString(String key) {
        Object obj = get(key);
        if (obj == null) {
            return null;
        }
        return obj.toString();
    }

    protected Number getNumber(String key) {
        Object obj = get(key);
        if (obj == null) {
            return null;
        } else if (obj instanceof Number number) {
            return number;
        }
        throw new IllegalStateException("unexpected type [" + obj.getClass().getName() + "] for [" + obj + "], expected Number");
    }

    protected void unsupported(String field, Boolean read) {
        throw new UnsupportedOperationException(
            read == null ? "" : (read ? "reading " : "writing ") + field + " is not supported for this action"
        );
    }
}
