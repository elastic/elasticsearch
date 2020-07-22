/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptFactory;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public abstract class IntegerScriptFieldScript extends AbstractNumberScriptFieldScript {

    public static final ScriptContext<IntegerScriptFieldScript.Factory> CONTEXT = new ScriptContext<>("integer_script_field", IntegerScriptFieldScript.Factory.class);

    public static final String[] PARAMETERS = {};

    static List<Whitelist> whitelist() {
        return List.of(WhitelistLoader.loadFromResourceFiles(RuntimeFieldsPainlessExtension.class, "integer_whitelist.txt"));
    }

    public interface Factory extends ScriptFactory {
        IntegerScriptFieldScript.LeafFactory newFactory(Map<String, Object> params, SearchLookup searchLookup);
    }

    public interface LeafFactory {
        IntegerScriptFieldScript newInstance(LeafReaderContext ctx) throws IOException;
    }

    private int[] values = new int[1];

    public IntegerScriptFieldScript(Map<String, Object> params, SearchLookup searchLookup, LeafReaderContext ctx) {
        super(params, searchLookup, ctx);
    }

    public final int[] values(){
        return values;
    }

    private void collectValue(int v) {
        if (values.length < count + 1) {
            values = ArrayUtil.grow(values, count + 1);
        }
        values[count++] = v;
    }

    public static class Value {
        private final IntegerScriptFieldScript script;

        public Value(IntegerScriptFieldScript script) {
            this.script = script;
        }

        public void value(int v) {
            script.collectValue(v);
        }
    }
}
