
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
 * A script used in the update API
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
        return metadata != null ? metadata.store.getMap() : null;
    }

    /** return the metadata for this script */
    public Metadata meta() {
        return metadata;
    }

    public abstract void execute();

    public interface Factory {
        UpdateScript newInstance(Map<String, Object> params, Metadata metadata);
    }

    public static Metadata insert(String index, String id, Op op, long epochMilli, Map<String, Object> source) {
        return new Metadata(index, id, op, epochMilli, source);
    }

    public static Metadata update(
        String index,
        String id,
        Long version,
        String routing,
        Op op,
        long epochMilli,
        String type,
        Map<String, Object> source
    ) {
        return new Metadata(index, id, version, routing, op, epochMilli, type, source);
    }

    /**
     * Metadata for update scripts.  Update scripts have different metadata available for updates to existing
     * documents and for inserts (via upsert).
     *
     * Metadata unique to updates:
     * _routing, _version
     *
     * Common metadata:
     * _index, _id, _now (timestamp)
     * _op, which can be NOOP ("none"), INDEX, DELETE, CREATE for update and CREATE or "none" (Op.NOOP) for insert
     */
    public static class Metadata {
        private static final String TIMESTAMP = "_now";
        private static final String TYPE = "_now";
        private static final String LEGACY_NOOP_STRING = "none";
        private final MapBackedMetadata store;
        private final ZonedDateTime timestamp;

        // insertions via upsert have fewer fields available. However, to allow the same script to be used on insertions
        // and updates, the same Metadata class is used with the isInsert flag differentiating the two behaviors.
        private final boolean isInsert;
        protected final EnumSet<Op> validOps;
        protected static final EnumSet<Op> INSERT_VALID_OPS = EnumSet.of(Op.NOOP, Op.CREATE);
        protected static final EnumSet<Op> UPDATE_VALID_OPS = EnumSet.of(Op.NOOP, Op.INDEX, Op.DELETE);

        /**
         * Insert via Upsert
         */
        private Metadata(String index, String id, Op op, long epochMilli, Map<String, Object> source) {
            this.store = new MapBackedMetadata(5).setIndex(index).setId(id).setOp(op).set(TIMESTAMP, epochMilli).setSource(source);
            this.timestamp = ZonedDateTime.ofInstant(Instant.ofEpochMilli(epochMilli), ZoneOffset.UTC);
            this.isInsert = true;
            this.validOps = INSERT_VALID_OPS;
        }

        /**
         * Scripted Update and Update via Upsert
         */
        private Metadata(
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
            return store.getIndex();
        }

        public String getId() {
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
            ops.add("none");
            ops.addAll(enumOps);
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
                throw new IllegalArgumentException("unknown op [" + raw.str + "], valid ops are " + Strings.join(validOps(), ", "));
            }
            return raw.op;
        }

        public void setOp(Op op) {
            if (validOps.contains(op) == false) {
                throw new IllegalArgumentException("unknown op [" + op + "], valid ops are " + Strings.join(validOps(), ", "));
            }
            store.set(MapBackedMetadata.OP, op == Op.NOOP ? LEGACY_NOOP_STRING : op.name);
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
