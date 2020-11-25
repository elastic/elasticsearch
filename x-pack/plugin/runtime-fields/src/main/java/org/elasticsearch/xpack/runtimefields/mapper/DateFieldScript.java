/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptFactory;
import org.elasticsearch.search.lookup.SearchLookup;

import java.util.List;
import java.util.Map;

public abstract class DateFieldScript extends AbstractLongFieldScript {
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
        DateFieldScript newInstance(LeafReaderContext ctx);
    }

    public static final Factory PARSE_FROM_SOURCE = (field, params, lookup, formatter) -> (LeafFactory) ctx -> new DateFieldScript(
        field,
        params,
        lookup,
        ctx
    ) {
        @Override
        public void execute() {
            for (Object v : extractFromSource(field)) {
                if (v instanceof String) {
                    try {
                        emit(formatter.parseMillis((String) v));
                    } catch (Exception e) {
                        // ignore
                    }
                }
            }
        }
    };

    public DateFieldScript(String fieldName, Map<String, Object> params, SearchLookup searchLookup, LeafReaderContext ctx) {
        super(fieldName, params, searchLookup, ctx);
    }

    public static class Emit {
        private final DateFieldScript script;

        public Emit(DateFieldScript script) {
            this.script = script;
        }

        public void emit(long v) {
            script.emit(v);
        }
    }
}
