
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import joptsimple.internal.Strings;

import org.elasticsearch.script.field.MapBackedMetadata;
import org.elasticsearch.script.field.Op;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Metadata
 *  RO: _index, _id, _routing, _version, _now (timestamp)
 *  RW: _op {@link org.elasticsearch.script.field.Op}, NOOP ("none"), INDEX, DELETE, CREATE
 */
public abstract class UpdateScript {

    public static final String[] PARAMETERS = {};

    /** The context used to compile {@link UpdateScript} factories. */
    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("update", Factory.class);

    /** The generic runtime parameters for the script. */
    private final Map<String, Object> params;

    private final Metadata metadata;

    public UpdateScript(Map<String, Object> params, Metadata metadata) {
        this.params = params;
        this.metadata = metadata;
    }

    /** Return the parameters for this script. */
    public Map<String, Object> getParams() {
        return params;
    }

    /** Return the update context for this script. */
    public Map<String, Object> getCtx() {
        if (metadata == null) {
            return null;
        }
        return metadata.store.getMap();
    }

    public Metadata meta() {
        return metadata;
    }

    public abstract void execute();

    public interface Factory {
        UpdateScript newInstance(Map<String, Object> params, Metadata metadata);
    }

    /**
     * Metadata for Updates via {@link UpdateScript} with script and upsert.
     *
     * _index, _id, _routing, _version and _now (timestamp) are read-only.
     *
     * _op is read/write with valid values: NOOP ("none"), INDEX, DELETE, CREATE
     *
     * _version_type is unavailable.
     */

    /**
     * Metadata for insertions done via scripted upsert with an {@link UpdateScript}
     *
     * The only metadata available is the timestamp and the Op.
     * The Op must be either "create" or "none" (Op.NOOP).
     *
     * _index, _id, _routing, _version, _version_type are unavailable.
     */
    public static class Metadata {
        private static final String TIMESTAMP = "_now";
        private static final String TYPE = "_now";
        private static final String LEGACY_NOOP_STRING = "none";
        private final MapBackedMetadata store;
        private final ZonedDateTime timestamp;

        // insertions via upsert have fewer fields available. However, to allow the same script to be used on insertions
        // and updates, the same Metadata class is used with the isInsert flag differniating the two behaviors.
        private final boolean isInsert;
        protected final EnumSet<Op> validOps;
        protected static final EnumSet<Op> INSERT_VALID_OPS = EnumSet.of(Op.NOOP, Op.CREATE);
        protected static final EnumSet<Op> UPDATE_VALID_OPS = EnumSet.of(Op.NOOP, Op.INDEX, Op.DELETE);

        // this is for context integration for compatibility with existing tests
        public Metadata(Map<String, Object> ctx) {
            this.store = new MapBackedMetadata(ctx);
            this.isInsert = store.getOp().op == Op.CREATE;
            this.validOps = isInsert ? INSERT_VALID_OPS : UPDATE_VALID_OPS;
            if (store.getMap().get(TIMESTAMP)instanceof Long epochMilli) {
                this.timestamp = ZonedDateTime.ofInstant(Instant.ofEpochMilli(epochMilli), ZoneOffset.UTC);
            } else {
                this.timestamp = null;
            }
        }

        public Metadata(Op op, long epochMilli, Map<String, Object> source) {
            this.store = new MapBackedMetadata(3).setOp(op).set(TIMESTAMP, epochMilli).setSource(source);
            this.timestamp = ZonedDateTime.ofInstant(Instant.ofEpochMilli(epochMilli), ZoneOffset.UTC);
            this.isInsert = true;
            this.validOps = INSERT_VALID_OPS;
        }

        public Metadata(
            String index,
            String id,
            Long version,
            String routing,
            Op op,
            long epochMilli,
            String type,
            Map<String, Object> source
        ) {
            this.store = new MapBackedMetadata(16).setIndex(index)
                .setId(id)
                .setVersion(version)
                .setRouting(routing)
                .setOp(op)
                .set(TIMESTAMP, epochMilli)
                .set(TYPE, type)
                .setSource(source);
            this.timestamp = ZonedDateTime.ofInstant(Instant.ofEpochMilli(epochMilli), ZoneOffset.UTC);
            this.isInsert = false;
            this.validOps = UPDATE_VALID_OPS;
        }

        public String getIndex() {
            if (isInsert) {
                throw new IllegalStateException("index unavailable on insert");
            }
            return store.getIndex();
        }

        public String getId() {
            if (isInsert) {
                throw new IllegalStateException("id unavailable for insert");
            }
            return store.getId();
        }

        public String getRouting() {
            if (isInsert) {
                throw new IllegalStateException("routing unavailable for insert");
            }
            return store.getRouting();
        }

        public Long getVersion() {
            if (isInsert) {
                throw new IllegalStateException("version unavailable for inserts");
            }
            return store.getVersion();
        }

        public List<String> validOps() {
            List<String> enumOps = validOps.stream().map(Op::getName).toList();
            List<String> ops = new ArrayList<>(enumOps.size() + 1);
            ops.addAll(enumOps);
            ops.add("none");
            return ops;
        }

        public Op getOp() {
            MapBackedMetadata.RawOp raw = store.getOp();
            if (raw.str == null) {
                throw new IllegalArgumentException("op must be non-null");
            }
            if (raw.str.toLowerCase(Locale.ROOT).equals(LEGACY_NOOP_STRING)) {
                return Op.NOOP;
            }
            if (validOps.contains(raw.op) == false) {
                throw new IllegalArgumentException("unknown op [" + raw.str + "], valid ops are " + Strings.join(validOps(), ","));
            }
            return raw.op;
        }

        public void setOp(Op op) {
            if (validOps.contains(op) == false) {
                throw new IllegalArgumentException("unknown op [" + op + "], valid ops are " + Strings.join(validOps(), ","));
            }
            store.setOp(op);
        }

        public ZonedDateTime getTimestamp() {
            return timestamp;
        }

        public String getType() {
            if (isInsert) {
                throw new IllegalStateException("type unavailable for inserts");
            }
            return store.getString(TYPE);
        }

        public Map<String, Object> getCtx() {
            return store.getMap();
        }

        public Map<String, Object> getSource() {
            return store.getSource();
        }
    }
}
