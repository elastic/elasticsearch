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

public abstract class DoubleScriptFieldScript extends AbstractScriptFieldScript {
    public static final ScriptContext<Factory> CONTEXT = newContext("double_script_field", Factory.class);

    static List<Whitelist> whitelist() {
        return List.of(WhitelistLoader.loadFromResourceFiles(RuntimeFieldsPainlessExtension.class, "double_whitelist.txt"));
    }

    public static final String[] PARAMETERS = {};

    public interface Factory extends ScriptFactory {
        LeafFactory newFactory(Map<String, Object> params, SearchLookup searchLookup);
    }

    public interface LeafFactory {
        DoubleScriptFieldScript newInstance(LeafReaderContext ctx) throws IOException;
    }

    public DoubleScriptFieldScript(Map<String, Object> params, SearchLookup searchLookup, LeafReaderContext ctx) {
        super(params, searchLookup, ctx);
    }

    public abstract double[] execute();

    /**
     * Execute the script for the provided {@code docId}.
     */
    public final double[] runForDoc(int docId) {
        setDocument(docId);
        return execute();
    }

    public static double[] convertFromDouble(double v) {
        return new double[] { v };
    }

    public static double[] convertFromCollection(Collection<?> v) {
        double[] result = new double[v.size()];
        int i = 0;
        for (Object o : v) {
            result[i++] = ((Number) o).doubleValue();
        }
        return result;
    }

    public static double[] convertFromDef(Object o) {
        if (o instanceof Number) {
            return convertFromDouble(((Number) o).doubleValue());
        } else if (o instanceof Collection) {
            return convertFromCollection((Collection<?>) o);
        } else {
            return (double[]) o;
        }
    }
}
