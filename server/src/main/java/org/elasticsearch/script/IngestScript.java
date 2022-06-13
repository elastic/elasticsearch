
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.script.field.MapBackedMetadata;

import java.time.ZonedDateTime;
import java.util.Locale;
import java.util.Map;

/**
 * A script used by the Ingest Script Processor.
 */
public abstract class IngestScript {

    public static final String[] PARAMETERS = {};

    /** The context used to compile {@link IngestScript} factories. */
    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>(
        "ingest",
        Factory.class,
        200,
        TimeValue.timeValueMillis(0),
        false,
        true
    );

    /** The generic runtime parameters for the script. */
    private final Map<String, Object> params;

    private final Metadata metadata;

    public IngestScript(Map<String, Object> params, Metadata metadata) {
        this.params = params;
        this.metadata = metadata;
    }

    /** Return the parameters for this script. */
    public Map<String, Object> getParams() {
        return params;
    }

    /** Provides backwards compatibility access to ctx */
    public Map<String, Object> getCtx() {
        return metadata != null ? metadata.store.getMap() : null;
    }

    /** Return the ingest metadata object */
    public Metadata metadata() {
        return metadata;
    }

    public abstract void execute();

    public interface Factory {
        IngestScript newInstance(Map<String, Object> params, Metadata metadata);
    }

    /**
     * Metadata available to scripts, backed by the ctx map.
     */
    public static class Metadata {
        private final MapBackedMetadata store;
        private final ZonedDateTime timestamp;
        public static final String VERSION_TYPE = "_version_type";

        public Metadata(Map<String, Object> ctx, ZonedDateTime timestamp) {
            store = new MapBackedMetadata(ctx);
            this.timestamp = timestamp;
        }

        public String getIndex() {
            return store.getIndex();
        }

        public void setIndex(String index) {
            store.setIndex(index);
        }

        public String getId() {
            return store.getId();
        }

        public void setId(String id) {
            store.setId(id);
        }

        public String getRouting() {
            return store.getRouting();
        }

        public void setRouting(String routing) {
            store.setRouting(routing);
        }

        public long getVersion() {
            Long version = store.getVersion();
            if (version == null) {
                return Long.MIN_VALUE;
            }
            return version;
        }

        public void setVersion(long version) {
            store.setVersion(version);
        }

        public String getVersionType() {
            return store.getString(VERSION_TYPE);
        }

        public void setVersionType(String versionType) {
            if (versionType != null) {
                versionType = versionType.toLowerCase(Locale.ROOT);
                // validation
                VersionType.fromString(versionType);
            }
            store.set(VERSION_TYPE, versionType);
        }

        public ZonedDateTime getTimestamp() {
            return timestamp;
        }
    }
}
