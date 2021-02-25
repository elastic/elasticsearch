
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
 * An update script.  Scripts of this type are used by {@link org.elasticsearch.index.reindex.ReindexAction} to modify documents during the
 * reindexing process.  This can be user-specified to mutate data during a reindex, or used internally as part of a data migration process.
 */
public abstract class UpdateScript {

    public static final String[] PARAMETERS = { };

    /** The context used to compile {@link UpdateScript} factories. */
    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("update", Factory.class);

    /** The generic runtime parameters for the script. */
    private final Map<String, Object> params;

    /**
     * The update context for the script. This will contain the information describing the document the script is acting on.  The script
     * should modify this map directly, and the reindex process will use the modified context to insert (NOT update) the document in the
     * new index.
     * <p>
     * This map should contain the following fields:
     * <ul>
     *     <li>{@link org.elasticsearch.index.mapper.IndexFieldMapper#NAME} - The index this document is stored in</li>
     *     <li>{@link org.elasticsearch.index.mapper.IdFieldMapper#NAME} - The id of the document</li>
     *     <li>{@link org.elasticsearch.index.mapper.VersionFieldMapper#NAME} - The version of the document, or -1</li>
     *     <li>{@link org.elasticsearch.index.mapper.RoutingFieldMapper#NAME} - The routing if set, otherwise null</li>
     *     <li>{@link org.elasticsearch.index.mapper.SourceFieldMapper#NAME} - The document source.  Modify this to change the values that
     *          will be indexed</li>
     * </ul>
     *
     * TODO: I can't make this an actual link
     * See AbstractAsyncBulkByScrollAction.ScriptApplier#apply()
     */
    private final Map<String, Object> ctx;

    public UpdateScript(Map<String, Object> params, Map<String, Object> ctx) {
        this.params = params;
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

    public abstract void execute();

    public interface Factory {
        UpdateScript newInstance(Map<String, Object> params, Map<String, Object> ctx);
    }
}
