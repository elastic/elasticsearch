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
import java.util.function.Consumer;
import java.util.function.IntPredicate;
import java.util.function.Predicate;

public abstract class StringScriptFieldScript extends AbstractScriptFieldScript {
    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("string_script_field", Factory.class);

    static List<Whitelist> whitelist() {
        return List.of(WhitelistLoader.loadFromResourceFiles(RuntimeFieldsPainlessExtension.class, "string_whitelist.txt"));
    }

    public static final String[] PARAMETERS = {};

    public interface Factory extends ScriptFactory {
        LeafFactory newFactory(Map<String, Object> params, SearchLookup searchLookup);
    }

    public interface LeafFactory {
        StringScriptFieldScript newInstance(LeafReaderContext ctx, Consumer<String> sync) throws IOException;

        /**
         * Wrap a {@link Predicate} that tests the result of the script. If the
         * script produces more than one value the result of the predicate is
         * {@code OR}ed together.
         */
        default IntPredicate wrapPredicate(LeafReaderContext ctx, Predicate<String> pred) throws IOException {
            return new IntPredicate() {
                boolean result;

                final StringScriptFieldScript script = newInstance(ctx, str -> {
                    result = result || pred.test(str);
                });

                @Override
                public boolean test(int docId) {
                    result = false;
                    script.setDocument(docId);
                    script.execute();
                    return result;
                }
            };
        }
    }

    private final Consumer<String> sync;

    public StringScriptFieldScript(Map<String, Object> params, SearchLookup searchLookup, LeafReaderContext ctx, Consumer<String> sync) {
        super(params, searchLookup, ctx);
        this.sync = sync;
    }

    public static class Value {
        private final StringScriptFieldScript script;

        public Value(StringScriptFieldScript script) {
            this.script = script;
        }

        public void value(String v) {
            script.sync.accept(v);
        }
    }
}
