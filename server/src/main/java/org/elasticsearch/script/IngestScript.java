
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

    private final Map<String, Object> ctx;

    private final Metadata metadata;

    public IngestScript(Map<String, Object> params, Map<String, Object> ctx, ZonedDateTime timestamp) {
        this.params = params;
        this.ctx = ctx;
        this.metadata = new Metadata(ctx, timestamp);
    }

    /** Return the parameters for this script. */
    public Map<String, Object> getParams() {
        return params;
    }

    public Map<String, Object> getCtx() {
        return ctx;
    }

    public Metadata meta() {
        return metadata;
    }

    public abstract void execute();

    public interface Factory {
        IngestScript newInstance(Map<String, Object> params, Map<String, Object> ctx, ZonedDateTime timestamp);
    }

    public static class Metadata extends org.elasticsearch.script.field.Metadata {
        private final ZonedDateTime timestamp;
        public static final String VERSION_TYPE = "_version_type";

        public Metadata(Map<String, Object> ctx, ZonedDateTime timestamp) {
            super(ctx);
            this.timestamp = timestamp;
        }

        public VersionType getVersionType() {
            String str = getString(VERSION_TYPE);
            if (str == null) {
                return null;
            }
            return VersionType.fromString(str.toLowerCase(Locale.ROOT));
        }

        public void setVersionType(VersionType versionType) {
            if (versionType == null) {
                ctx.put(VERSION_TYPE, null);
            } else {
                ctx.put(VERSION_TYPE, VersionType.toString(versionType));
            }
        }

        public ZonedDateTime getTimestamp() {
            return timestamp;
        }
    }
}
