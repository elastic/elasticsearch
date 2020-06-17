/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptFactory;
import org.elasticsearch.search.lookup.DocLookup;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.Map;

public abstract class StringScriptFieldsScript extends AbstractScriptFieldsScript {
    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("string_script_field", Factory.class);
    public static final String[] PARAMETERS = {};

    public interface Factory extends ScriptFactory {
        LeafFactory newFactory(Map<String, Object> params, SourceLookup source, DocLookup fieldData);
    }
    public static interface LeafFactory {
        StringScriptFieldsScript newInstance(LeafReaderContext ctx) throws IOException;
    }

    public StringScriptFieldsScript(Map<String, Object> params, SourceLookup source, DocLookup fieldData, LeafReaderContext ctx) {
        super(params, source, fieldData, ctx);
    }

    public abstract String execute();
}
