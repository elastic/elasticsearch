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
import java.util.function.DoubleConsumer;

public abstract class DoubleScriptFieldScript extends AbstractScriptFieldScript {
    static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("double_script_field", Factory.class);

    static List<Whitelist> whitelist() {
        return List.of(WhitelistLoader.loadFromResourceFiles(RuntimeFieldsPainlessExtension.class, "double_whitelist.txt"));
    }

    public static final String[] PARAMETERS = {};

    public interface Factory extends ScriptFactory {
        LeafFactory newFactory(Map<String, Object> params, SearchLookup searchLookup);
    }

    public interface LeafFactory {
        DoubleScriptFieldScript newInstance(LeafReaderContext ctx, DoubleConsumer sync) throws IOException;
    }

    private final DoubleConsumer sync;

    public DoubleScriptFieldScript(Map<String, Object> params, SearchLookup searchLookup, LeafReaderContext ctx, DoubleConsumer sync) {
        super(params, searchLookup, ctx);
        this.sync = sync;
    }

    public static class Value {
        private final DoubleScriptFieldScript script;

        public Value(DoubleScriptFieldScript script) {
            this.script = script;
        }

        public void value(double v) {
            script.sync.accept(v);
        }
    }
}
