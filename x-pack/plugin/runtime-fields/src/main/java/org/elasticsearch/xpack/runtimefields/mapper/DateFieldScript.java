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

import java.time.Instant;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
        formatter,
        ctx
    ) {
        @Override
        public void execute() {
            for (Object v : extractFromSource(field)) {
                try {
                    emit(formatter.parseMillis(Objects.toString(v)));
                } catch (Exception e) {
                    // ignore
                }
            }
        }
    };

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

    public static void emit(Integer receiver, DateFieldScript script) {
        script.emit(receiver);
    }

    public static void emit(Long receiver, DateFieldScript script) {
        script.emit(receiver);
    }

    public static void emit(String receiver, DateFieldScript script) {
        script.emit(script.formatter.parseMillis(receiver));
    }

    public static void emit(TemporalAccessor receiver, DateFieldScript script) {
        long millis = millis(receiver);
        assert millis == Instant.from(receiver).toEpochMilli();
        script.emit(millis);
    }

    /**
     * Extract millis since epoch from a {@link TemporalAccessor}.
     * <p>
     * Should return the same thing as calling {@code Instant.from(accessor).toEpochMilli()}
     * but without quite as much ceremony.
     */
    static long millis(TemporalAccessor accessor) {
        return accessor.getLong(ChronoField.INSTANT_SECONDS) * 1000 + accessor.get(ChronoField.NANO_OF_SECOND) / 1_000_000;
    }
}
