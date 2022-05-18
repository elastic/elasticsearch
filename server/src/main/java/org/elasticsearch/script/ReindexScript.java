
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.script.field.Metadata;

import java.util.Map;

/**
 * A script for reindex.
 *
 * Metadata
 *   RW: _index (non-null), _id, _routing, _version, _op {@link org.elasticsearch.script.field.Op} One of NOOP, INDEX, DELETE
 */
public abstract class ReindexScript {

    public static final String[] PARAMETERS = {};

    /** The context used to compile {@link ReindexScript} factories. */
    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("reindex", Factory.class);

    /** The generic runtime parameters for the script. */
    private final Map<String, Object> params;

    /** The metadata about the update available to the script */
    private final Metadata metadata;

    /** The update context for the script. */
    private final Map<String, Object> ctx;

    public ReindexScript(Map<String, Object> params, Metadata metadata, Map<String, Object> ctx) {
        this.params = params;
        this.metadata = metadata;
        this.ctx = ctx;
    }

    /** Return the parameters for this script. */
    public Map<String, Object> getParams() {
        return params;
    }

    /** Return the update context for this script. */
    public Map<String, Object> getCtx() {
        return ctx;
    }

    public Metadata meta() {
        return metadata;
    }

    public abstract void execute();

    public interface Factory {
        ReindexScript newInstance(Map<String, Object> params, Metadata metadata, Map<String, Object> ctx);
    }
}
