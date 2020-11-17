/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
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

    private final DateFormatter formatter;

    public DateFieldScript(
        String fieldName,
        Map<String, Object> params,
        SearchLookup searchLookup,
        DateFormatter formatter,
        LeafReaderContext ctx
    ) {
        super(fieldName, params, searchLookup, ctx);
        this.formatter = formatter;
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

    /**
     * Temporary parse method that takes into account the date format. We'll
     * remove this when we have "native" source parsing fields.
     */
    public static class Parse {
        private final DateFieldScript script;

        public Parse(DateFieldScript script) {
            this.script = script;
        }

        public long parse(Object str) {
            return script.formatter.parseMillis(str.toString());
        }
    }

    public static class Values {
        private final DateFieldScript script;

        public Values(DateFieldScript script) {
            this.script = script;
        }

        public List<Object> values(String path) {
            return XContentMapValues.extractRawValues(path, script.getSource());
        }
    }

    public static long tryParseDate(Object val, DateFormatter formatter) {
        if (val instanceof String) {
            return formatter.parseMillis((String)val);
        }
        throw new IllegalArgumentException("Cannot parse value " + val + " as date");
    }
}
