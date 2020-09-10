/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptFactory;
import org.elasticsearch.search.lookup.SearchLookup;

import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.List;
import java.util.Map;

public abstract class DateScriptFieldScript extends AbstractLongScriptFieldScript {
    public static final ScriptContext<Factory> CONTEXT = newContext("date", Factory.class);

    static List<Whitelist> whitelist() {
        return List.of(WhitelistLoader.loadFromResourceFiles(RuntimeFieldsPainlessExtension.class, "date_whitelist.txt"));
    }

    @SuppressWarnings("unused")
    public static final String[] PARAMETERS = {};

    public interface Factory extends ScriptFactory {
        LeafFactory newFactory(String fieldName, Map<String, Object> params, SearchLookup searchLookup, DateFormatter formatter);
    }

    public interface LeafFactory {
        DateScriptFieldScript newInstance(LeafReaderContext ctx);
    }

    private final DateFormatter formatter;

    public DateScriptFieldScript(
        String fieldName,
        Map<String, Object> params,
        SearchLookup searchLookup,
        DateFormatter formatter,
        LeafReaderContext ctx
    ) {
        super(fieldName, params, searchLookup, ctx);
        this.formatter = formatter;
    }

    public static long toEpochMilli(TemporalAccessor v) {
        // TemporalAccessor is a nanos API so we have to convert.
        long millis = Math.multiplyExact(v.getLong(ChronoField.INSTANT_SECONDS), 1000);
        millis = Math.addExact(millis, v.get(ChronoField.NANO_OF_SECOND) / 1_000_000);
        return millis;
    }

    public static class Emit {
        private final DateScriptFieldScript script;

        public Emit(DateScriptFieldScript script) {
            this.script = script;
        }

        public void emit(long v) {
            script.emit(v);
        }
    }

    public static class Parse {
        private final DateScriptFieldScript script;

        public Parse(DateScriptFieldScript script) {
            this.script = script;
        }

        public long parse(Object str) {
            return script.formatter.parseMillis(str.toString());
        }
    }
}
