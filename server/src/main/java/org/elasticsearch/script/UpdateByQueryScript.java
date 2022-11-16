
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import java.util.Map;

/**
 * A script used by the update by query api
 */
public abstract class UpdateByQueryScript extends WriteScript {

    public static final String[] PARAMETERS = {};

    /** The context used to compile {@link UpdateByQueryScript} factories. */
    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("update_by_query", Factory.class);

    /** The generic runtime parameters for the script. */
    private final Map<String, Object> params;

    public UpdateByQueryScript(Map<String, Object> params, CtxMap<UpdateByQueryMetadata> ctxMap) {
        super(ctxMap);
        this.params = params;
    }

    /** Return the parameters for this script. */
    public Map<String, Object> getParams() {
        return params;
    }

    public abstract void execute();

    public interface Factory {
        UpdateByQueryScript newInstance(Map<String, Object> params, CtxMap<UpdateByQueryMetadata> ctx);
    }
}
