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
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
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
            unsupported(INDEX, false);
        }
        return getString(indexKey);
    }

    public void setIndex(String index) {
        if (indexKey == null) {
            unsupported(INDEX, false);
        }
        put(indexKey, index);
    }

    public String getId() {
        if (idKey == null) {
            unsupported(ID, false);
        }
        return getString(idKey);
    }

    public void setId(String id) {
        if (idKey == null) {
            unsupported(ID, false);
        }
        put(idKey, id);
    }

    public String getRouting() {
        if (routingKey == null) {
            unsupported(ROUTING, false);
        }
        return getString(routingKey);
    }

    public void setRouting(String routing) {
        if (routingKey == null) {
            unsupported(ROUTING, false);
        }
        put(routingKey, routing);
    }

    public Long getVersion() {
        if (versionKey == null) {
            unsupported(VERSION, false);
        }
        Number version = getNumber(versionKey);
        if (version == null) {
            return null;
        }
        long lng = version.longValue();
        if (version.doubleValue() != lng) {
            // did we round?
            throw new IllegalArgumentException("version may only be set to an int or a long but was [" + version + "]");
        }
        return lng;
    }

    public void setVersion(Long version) {
        if (versionKey == null) {
            unsupported(VERSION, false);
        }
        put(versionKey, version);
    }

    public VersionType getVersionType() {
        if (versionTypeKey == null) {
            unsupported(VERSION_TYPE, false);
        }
        String str = getString(versionTypeKey);
        if (str == null) {
            return null;
        }
        return VersionType.fromString(str.toLowerCase(Locale.ROOT));
    }

    public void setVersionType(VersionType versionType) {
        if (versionTypeKey == null) {
            unsupported(VERSION_TYPE, false);
        }
        if (versionType == null) {
            put(versionTypeKey, null);
        } else {
            put(versionTypeKey, VersionType.toString(versionType));
        }
    }

    protected Op opFromString(String opStr) {
        return Op.fromString(opStr);
    }

    public Op getOp() {
        Object raw = get(opKey);
        if (raw == null) {
            throw new IllegalArgumentException("Operation type must be non-null");
        }

        Op op;
        String str;

        if (raw instanceof Op rawOp) {
            str = rawOp.name;
            op = rawOp;
        } else if (raw instanceof String rawStr) {
            str = rawStr;
            op = opFromString(str);
        } else {
            throw new IllegalArgumentException(
                "Invalid type [" + raw.getClass().getName() + "] for Op [" + raw + "], expected String or Op"
            );
        }

        if (isValidOp(op) == false) {
            throw new IllegalArgumentException("Operation type [" + str + "] not allowed, only " + validOps() + " are allowed");
        }

        return op;
    }

    public void setOp(Op op) {
        validateOp(op);
        // to string keeps compatibility with existing string APIs
        put(opKey, op.toString());
    }

    protected boolean isValidOp(Op op) {
        return op != null && (VALID_OPS == null || VALID_OPS.contains(op));
    }

    protected void validateOp(Op op) {
        if (op == null) {
            throw new IllegalStateException("Operation type must be non-null");
        } else if (VALID_OPS != null && VALID_OPS.contains(op) == false) {
            throw new IllegalArgumentException("Operation type [" + op.name + "] not allowed, only " + validOps() + " are allowed");
        }
    }

    public List<String> validOps() {
        if (VALID_OPS == null) {
            return Collections.emptyList();
        }
        return VALID_OPS.stream().map(Op::getName).sorted(Comparator.reverseOrder()).toList();
    }

    public ZonedDateTime getTimestamp() {
        unsupported(TIMESTAMP, false);
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
        throw new IllegalArgumentException("unexpected type [" + obj.getClass().getName() + "] for [" + obj + "], expected Number");
    }

    protected void unsupported(String field, boolean write) {
        throw new UnsupportedOperationException((write ? "writing " : "") + field + " is not supported for this action");
    }

    /**
     * Is the key a metadata key?
     */
    public boolean isMetadataKey(String key) {
        // This uses equality checks rather than a Set to avoid Set construction cost because it is expected to be rarely used.
        return isKey(indexKey, key)
            || isKey(idKey, key)
            || isKey(routingKey, key)
            || isKey(versionKey, key)
            || isKey(versionTypeKey, key)
            || isKey(opKey, key);
    }

    protected static boolean isKey(String key, String that) {
        return key != null && key.equals(that);
    }
}
