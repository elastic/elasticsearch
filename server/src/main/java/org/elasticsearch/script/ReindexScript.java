
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script;

import java.util.Map;

/**
 * A script used in the reindex api
 */
public abstract class ReindexScript extends WriteScript {

    public static final String[] PARAMETERS = {};

    /** The context used to compile {@link ReindexScript} factories. */
    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("reindex", Factory.class);

    /** The generic runtime parameters for the script. */
    private final Map<String, Object> params;

    /**
     * Metadata available to the script
     * _index can't be null
     * _id, _routing, _slice and _version are writable and nullable
     * op must be 'noop', 'index' or 'delete'
     */
    public ReindexScript(Map<String, Object> params, CtxMap<ReindexMetadata> ctxMap) {
        super(ctxMap);
        this.params = params;
    }

    /** Return the parameters for this script. */
    public Map<String, Object> getParams() {
        return params;
    }

    private Runnable cancellationCheck = null;

    public void _setCancellationCheck(Runnable cancellationCheck) {
        this.cancellationCheck = cancellationCheck;
    }

    public Runnable _getCancellationCheck() {
        return cancellationCheck;
    }

    public abstract void execute();

    public interface Factory {
        ReindexScript newInstance(Map<String, Object> params, CtxMap<ReindexMetadata> ctxMap);
    }
}
