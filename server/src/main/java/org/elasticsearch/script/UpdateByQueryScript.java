
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.script.field.AbstractBulkMetadata;
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
    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>(
        "update_by_query",
        Factory.class,
        200,
        TimeValue.timeValueMillis(0),
        false,
        true
    );

    /** The generic runtime parameters for the script. */
    private final Map<String, Object> params;

    private Metadata metadata;

    public UpdateByQueryScript(Map<String, Object> params) {
        this.params = params;
    }

    /** Return the parameters for this script. */
    public Map<String, Object> getParams() {
        return params;
    }

    public void setMetadata(Metadata metadata) {
        this.metadata = metadata;
    }

    public Metadata meta() {
        return metadata;
    }

    public Map<String, Object> getCtx() {
        if (metadata == null) {
            return null;
        }
        return metadata.getCtx();
    }

    public abstract void execute();

    public interface Factory {
        UpdateByQueryScript newInstance(Map<String, Object> params);
    }

    public static class Metadata extends AbstractBulkMetadata {
        public Metadata(String index, String id, Long version, String routing, Op op, Map<String, Object> source) {
            super(index, id, version, routing, op, source);
        }
    }
}
