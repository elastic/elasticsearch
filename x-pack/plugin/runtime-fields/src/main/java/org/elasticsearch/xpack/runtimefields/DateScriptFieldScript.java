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
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.List;
import java.util.Map;

public abstract class DateScriptFieldScript extends AbstractLongScriptFieldScript {
    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("date_script_field", Factory.class);

    static List<Whitelist> whitelist() {
        return List.of(WhitelistLoader.loadFromResourceFiles(RuntimeFieldsPainlessExtension.class, "date_whitelist.txt"));
    }

    public static final String[] PARAMETERS = {};

    public interface Factory extends ScriptFactory {
        LeafFactory newFactory(Map<String, Object> params, SearchLookup searchLookup);
    }

    public interface LeafFactory {
        DateScriptFieldScript newInstance(LeafReaderContext ctx) throws IOException;
    }

    public DateScriptFieldScript(Map<String, Object> params, SearchLookup searchLookup, LeafReaderContext ctx) {
        super(params, searchLookup, ctx);
    }

    public static class Millis {
        private final DateScriptFieldScript script;

        public Millis(DateScriptFieldScript script) {
            this.script = script;
        }

        public void millis(long v) {
            script.collectValue(v);
        }
    }

    public static class Date {
        private final DateScriptFieldScript script;

        public Date(DateScriptFieldScript script) {
            this.script = script;
        }

        public void date(TemporalAccessor v) {
            // TemporalAccessor is a nanos API so we have to convert.
            long millis = Math.multiplyExact(v.getLong(ChronoField.INSTANT_SECONDS), 1000);
            millis = Math.addExact(millis, v.get(ChronoField.NANO_OF_SECOND) / 1_000_000);
            script.collectValue(millis);
        }
    }

}
