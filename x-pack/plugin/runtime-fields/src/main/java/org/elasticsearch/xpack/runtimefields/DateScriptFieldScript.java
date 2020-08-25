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

import java.io.IOException;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public abstract class DateScriptFieldScript extends AbstractLongScriptFieldScript {
    public static final ScriptContext<Factory> CONTEXT = newContext("date", Factory.class);

    static List<Whitelist> whitelist() {
        return List.of(WhitelistLoader.loadFromResourceFiles(RuntimeFieldsPainlessExtension.class, "date_whitelist.txt"));
    }

    public static final String[] PARAMETERS = {};

    public interface Factory extends ScriptFactory {
        LeafFactory newFactory(Map<String, Object> params, SearchLookup searchLookup, DateFormatter formatter);
    }

    public interface LeafFactory {
        DateScriptFieldScript newInstance(LeafReaderContext ctx) throws IOException;
    }

    private final DateFormatter formatter;

    public DateScriptFieldScript(Map<String, Object> params, SearchLookup searchLookup, DateFormatter formatter, LeafReaderContext ctx) {
        super(params, searchLookup, ctx);
        this.formatter = formatter;
    }

    public static long[] convertFromLong(long v) {
        return new long[] { v };
    }

    public static long[] convertFromTemporalAccessor(TemporalAccessor v) {
        return new long[] { convertSingleTemporalAccessor(v) };
    }

    public static long[] convertFromTemporalAccessorArray(TemporalAccessor[] v) {
        long[] result = new long[v.length];
        int i = 0;
        for (TemporalAccessor o : v) {
            result[i++] = convertSingleTemporalAccessor(o);
        }
        return result;
    }

    public static long[] convertFromCollection(Collection<?> v) {
        long[] result = new long[v.size()];
        int i = 0;
        for (Object o : v) {
            try {
                result[i++] = convertCollectionElement(o);
            } catch (ClassCastException e) {
                throw new ClassCastException("Exception casting collection member [" + o + "]: " + e.getMessage());
            }
        }
        return result;
    }

    public static long[] convertFromDef(Object o) {
        if (o instanceof Long) {
            return convertFromLong(((Long) o).longValue());
        }
        if (o instanceof TemporalAccessor) {
            return convertFromTemporalAccessor((TemporalAccessor) o);
        }
        if (o instanceof long[]) {
            return (long[]) o;
        }
        if (o instanceof TemporalAccessor[]) {
            return convertFromTemporalAccessorArray((TemporalAccessor[]) o);
        }
        if (o instanceof Integer) {
            return convertFromLong(((Integer) o).longValue());
        }
        if (o instanceof Short) {
            return convertFromLong(((Short) o).longValue());
        }
        if (o instanceof Byte) {
            return convertFromLong(((Byte) o).longValue());
        }
        if (o instanceof Collection) {
            return convertFromCollection((Collection<?>) o);
        }
        throw new ClassCastException(
            "Can't cast ["
                + o.getClass().getName()
                + "] to long, long[], TemporalAccessor, TemporalAccessor[], int, short, byte, or a collection"
        );
    }

    private static long convertCollectionElement(Object o) {
        if (o instanceof Long) {
            return ((Long) o).longValue();
        }
        if (o instanceof TemporalAccessor) {
            return convertSingleTemporalAccessor((TemporalAccessor) o);
        }
        if (o instanceof Integer) {
            return ((Integer) o).longValue();
        }
        if (o instanceof Short) {
            return ((Short) o).longValue();
        }
        if (o instanceof Byte) {
            return ((Byte) o).longValue();
        }
        throw new ClassCastException("Can't cast [" + o.getClass().getName() + "] to long, TemporalAccessor, int, short, or byte");
    }

    private static long convertSingleTemporalAccessor(TemporalAccessor v) {
        long millis = Math.multiplyExact(v.getLong(ChronoField.INSTANT_SECONDS), 1000);
        return Math.addExact(millis, v.get(ChronoField.NANO_OF_SECOND) / 1_000_000);
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
