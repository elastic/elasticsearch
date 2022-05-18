
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.script.field.Metadata;
import org.elasticsearch.script.field.Op;

import java.util.Map;

/**
 * A script for update by query.
 *
 * Metadata
 *   RO: _index, _id, _version, _routing
 *   RW: _op {@link Op} INDEX, NOOP, DELETE
 */
public abstract class UpdateByQueryScript {

    public static final String[] PARAMETERS = {};

    /** The context used to compile {@link UpdateByQueryScript} factories. */
    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("update_by_query", Factory.class);

    /** The generic runtime parameters for the script. */
    private final Map<String, Object> params;

    /** The metadata about the update available to the script */
    private final Metadata metadata = new UpdateByQueryMetadata();

    public UpdateByQueryScript(Map<String, Object> params) {
        this.params = params;
    }

    /** Return the parameters for this script. */
    public Map<String, Object> getParams() {
        return params;
    }

    /** Return the update context for this script. */
    public Map<String, Object> getCtx() {
        return metadata.getCtx();
    }

    public UpdateByQueryMetadata meta() {
        return metadata;
    }

    public abstract void execute();

    public interface Factory {
        UpdateByQueryScript newInstance(Map<String, Object> params);
    }

    public static class UpdateByQueryMetadata extends Metadata {
        private Env env;

        public UpdateByQueryMetadata() {
            // EnumSet.of(Op.NOOP, Op.INDEX, Op.DELETE)
            super();
        }

        public void setEnv(Env env) {
            this.env = env;
        }

        public String getIndex() {

        }

        public String getId() {

        }

        public String getRouting() {

        }

        public Long getVersion() {

        }

        public Op getOp() {

        }

        public void setOp(Op op) {

        }

        public Map<String, Object> getCtx() {
            return env.ctx;
        }
    }

    public static class Env {
        public final String index;
        public final String id;
        public final String routing;
        public final Long version;
        public final Op op;
        public final Map<String, Object> source;
        public final Map<String, Object> ctx;

        public Env(String index, String id, String routing, Long version, Op op, Map<String, Object> source) {
            this.index = index;
            this.id = id;
            this.routing = routing;
            this.version = version;
            this.op = op;
            this.source = source;
            this.ctx = Maps.newMapWithExpectedSize(7);
            ctx.put(UpdateByQueryMetadata.SOURCE, source);
            ctx.put(UpdateByQueryMetadata.INDEX, index);
            ctx.put(UpdateByQueryMetadata.ID, id);
            ctx.put(UpdateByQueryMetadata.ROUTING, routing);
            ctx.put(UpdateByQueryMetadata.VERSION, version);
            // TODO(stu): set
            ctx.put(UpdateByQueryMetadata.OP, op);

        }
    }
}
