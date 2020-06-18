/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptFactory;
import org.elasticsearch.search.lookup.DocLookup;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.Map;
import java.util.function.LongConsumer;

public abstract class LongScriptFieldScript extends AbstractScriptFieldsScript {
    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("long_script_field", Factory.class);
    public static final String[] PARAMETERS = {};

    public interface Factory extends ScriptFactory {
        LeafFactory newFactory(Map<String, Object> params, SourceLookup source, DocLookup fieldData);
    }
    public static interface LeafFactory {
        LongScriptFieldScript newInstance(LeafReaderContext ctx, LongConsumer sync) throws IOException;
    }

    private final LongConsumer sync;

    public LongScriptFieldScript(
        Map<String, Object> params,
        SourceLookup source,
        DocLookup fieldData,
        LeafReaderContext ctx,
        LongConsumer sync
    ) {
        super(params, source, fieldData, ctx);
        this.sync = sync;
    }

    /**
     * Expose the consumer to the script.
     * <p>
     * This is temporary and I'll remove it in the next PR when I figure out class methods.
     */
    public LongConsumer getSync() {
        return sync;
    }
}
