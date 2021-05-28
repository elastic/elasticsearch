/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.lookup.SearchLookup;

import java.util.Map;

public abstract class LongFieldScript extends AbstractLongFieldScript {
    public static final ScriptContext<Factory> CONTEXT = newContext("long_field", Factory.class);

    public static final LongFieldScript.Factory PARSE_FROM_SOURCE
        = (field, params, lookup) -> (LongFieldScript.LeafFactory) ctx -> new LongFieldScript
        (
            field,
            params,
            lookup,
            ctx
        ) {
        @Override
        public void execute() {
            emitFromSource();
        }
    };

    @SuppressWarnings("unused")
    public static final String[] PARAMETERS = {};

    public interface Factory extends ScriptFactory {
        LeafFactory newFactory(String fieldName, Map<String, Object> params, SearchLookup searchLookup);
    }

    public interface LeafFactory {
        LongFieldScript newInstance(LeafReaderContext ctx);
    }

    public LongFieldScript(String fieldName, Map<String, Object> params, SearchLookup searchLookup, LeafReaderContext ctx) {
        super(fieldName, params, searchLookup, ctx);
    }

    @Override
    protected void emitFromObject(Object v) {
        try {
            emit(NumberFieldMapper.NumberType.objectToLong(v, true));
        } catch (Exception e) {
            // ignore;
        }
    }

    public static class Emit {
        private final LongFieldScript script;

        public Emit(LongFieldScript script) {
            this.script = script;
        }

        public void emit(long v) {
            script.emit(v);
        }
    }
}
