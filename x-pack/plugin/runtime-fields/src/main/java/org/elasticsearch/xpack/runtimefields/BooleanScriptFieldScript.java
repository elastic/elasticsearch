/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptFactory;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public abstract class BooleanScriptFieldScript extends AbstractScriptFieldScript {
    public static final ScriptContext<Factory> CONTEXT = newContext("boolean_script_field", Factory.class);

    static List<Whitelist> whitelist() {
        return List.of(WhitelistLoader.loadFromResourceFiles(RuntimeFieldsPainlessExtension.class, "boolean_whitelist.txt"));
    }

    public static final String[] PARAMETERS = {};

    public interface Factory extends ScriptFactory {
        LeafFactory newFactory(Map<String, Object> params, SearchLookup searchLookup);
    }

    public interface LeafFactory {
        BooleanScriptFieldScript newInstance(LeafReaderContext ctx) throws IOException;
    }

    public BooleanScriptFieldScript(Map<String, Object> params, SearchLookup searchLookup, LeafReaderContext ctx) {
        super(params, searchLookup, ctx);
    }

    public abstract boolean[] execute();

    /**
     * Execute the script for the provided {@code docId}.
     */
    public final boolean[] runForDoc(int docId) {
        setDocument(docId);
        return execute();
    }

    public static boolean parse(Object str) {
        return Booleans.parseBoolean(str.toString());
    }

    public static boolean[] convertFromBoolean(boolean v) {
        return new boolean[] { v };
    }

    public static boolean[] convertFromCollection(Collection<?> v) {
        boolean[] result = new boolean[v.size()];
        int i = 0;
        for (Object o : v) {
            result[i++] = (Boolean) o;
        }
        return result;
    }

    public static boolean[] convertFromDef(Object o) {
        if (o instanceof Boolean) {
            return convertFromBoolean((Boolean) o);
        } else if (o instanceof Collection) {
            return convertFromCollection((Collection<?>) o);
        } else {
            return (boolean[]) o;
        }
    }
}
