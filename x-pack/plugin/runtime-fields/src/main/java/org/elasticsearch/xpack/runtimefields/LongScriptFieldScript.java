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
import java.util.Collection;
import java.util.List;
import java.util.Map;

public abstract class LongScriptFieldScript extends AbstractLongScriptFieldScript {
    public static final ScriptContext<Factory> CONTEXT = newContext("long_script_field", Factory.class);

    static List<Whitelist> whitelist() {
        return List.of(WhitelistLoader.loadFromResourceFiles(RuntimeFieldsPainlessExtension.class, "long_whitelist.txt"));
    }

    public static final String[] PARAMETERS = {};

    public interface Factory extends ScriptFactory {
        LeafFactory newFactory(Map<String, Object> params, SearchLookup searchLookup);
    }

    public interface LeafFactory {
        LongScriptFieldScript newInstance(LeafReaderContext ctx) throws IOException;
    }

    public LongScriptFieldScript(Map<String, Object> params, SearchLookup searchLookup, LeafReaderContext ctx) {
        super(params, searchLookup, ctx);
    }

    public static long[] convertFromLong(long v) {
        return new long[] { v };
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
        if (o instanceof long[]) {
            return (long[]) o;
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
        throw new ClassCastException("Can't cast [" + o.getClass().getName() + "] to long, long[], int, short, byte, or a collection");
    }

    private static long convertCollectionElement(Object o) {
        if (o instanceof Long) {
            return ((Long) o).longValue();
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
        throw new ClassCastException("Can't cast [" + o.getClass().getName() + "] to long, int, short, or byte");
    }
}
