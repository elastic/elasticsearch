/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptFactory;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.LongConsumer;

public abstract class LongScriptFieldScript extends AbstractScriptFieldScript {
    static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("long_script_field", Factory.class);

    static List<Whitelist> whitelist() {
        return List.of(WhitelistLoader.loadFromResourceFiles(RuntimeFieldsPainlessExtension.class, "long_whitelist.txt"));
    }

    /**
     * Magic constant that painless needs to name the parameters. There aren't any so it is empty.
     */
    public static final String[] PARAMETERS = {};

    /**
     * Factory for building instances of the script for a particular search context.
     */
    public interface Factory extends ScriptFactory {
        LeafFactory newFactory(Map<String, Object> params, SearchLookup searchLookup);
    }

    /**
     * Factory for building the script for a particular leaf or for building
     * runtime values which manages the creation of doc values and queries.
     */
    public interface LeafFactory {
        /**
         * Build a new script.
         */
        LongScriptFieldScript newInstance(LeafReaderContext ctx, LongConsumer sync) throws IOException;

        /**
         * Build an {@link LongRuntimeFieldHelper} to manage creation of doc
         * values and queries using the script.
         */
        default LongRuntimeFieldHelper runtimeFieldHelper() {
            return new LongRuntimeFieldHelper((ctx, sync) -> {
                LongScriptFieldScript script = newInstance(ctx, sync);
                return docId -> {
                    script.setDocId(docId);
                    script.execute();
                };
            });
        }
    }

    private final LongConsumer sync;

    public LongScriptFieldScript(Map<String, Object> params, SearchLookup searchLookup, LeafReaderContext ctx, LongConsumer sync) {
        super(params, searchLookup, ctx);
        this.sync = sync;
    }

    public static class Value {
        private final LongScriptFieldScript script;

        public Value(LongScriptFieldScript script) {
            this.script = script;
        }

        public void value(long v) {
            script.sync.accept(v);
        }
    }
}
