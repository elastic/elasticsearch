/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.search.lookup.SearchLookup;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//TODO expose this context to the painless execute API
public abstract class CompositeFieldScript extends AbstractFieldScript {
    public static final ScriptContext<CompositeFieldScript.Factory> CONTEXT = newContext("composite_field", Factory.class);

    @SuppressWarnings("unused")
    public static final String[] PARAMETERS = {};

    public interface Factory extends ScriptFactory {
        CompositeFieldScript.LeafFactory newFactory(String fieldName, Map<String, Object> params, SearchLookup searchLookup);
    }

    public interface LeafFactory {
        CompositeFieldScript newInstance(LeafReaderContext ctx);
    }

    private final Map<String, List<Object>> fieldValues = new HashMap<>();

    public CompositeFieldScript(String fieldName, Map<String, Object> params, SearchLookup searchLookup, LeafReaderContext ctx) {
        super(fieldName, params, searchLookup, ctx);
    }

    /**
     * Runs the object script and returns the values that were emitted for the provided field name
     * @param field the field name to extract values from
     * @return the values that were emitted for the provided field
     */
    public final List<Object> getValues(String field) {
        //TODO for now we re-run the script every time a leaf field is accessed, but we could cache the values?
        fieldValues.clear();
        execute();
        return fieldValues.get(field);
    }

    protected final void emit(String field, Object value) {
        //fields will be emitted without the prefix, yet they will be looked up using their full name, hence we store the full name
        List<Object> values = this.fieldValues.computeIfAbsent(fieldName + "." + field, s -> new ArrayList<>());
        values.add(value);
    }

    @Override
    protected void emitFromObject(Object v) {
        throw new UnsupportedOperationException();
    }

    public static class EmitField {
        private final CompositeFieldScript script;

        public EmitField(CompositeFieldScript script) {
            this.script = script;
        }

        /**
         * Emits a value for the provided field. Note that ideally we would have typed the value, and have
         * one emit per supported data type, but the arity in Painless does not take arguments type into account, only method name and
         * number of arguments. That means that we would have needed a different method name per type, and given that we need the Object
         * variant anyways to be able to emit an entire map, we went for taking an object also for the keyed emit variant.
         *
         * @param field the field name
         * @param value the value
         */
        public void emit(String field, Object value) {
            script.emit(field, value);
        }
    }

    public static class EmitMap {
        private final CompositeFieldScript script;

        public EmitMap(CompositeFieldScript script) {
            this.script = script;
        }

        /**
         * Emits all the subfields in one go. The key in the provided map is the field name, and the value their value(s)
         * @param subfields the map that holds the key-value pairs
         */
        public void emit(Map<String, Object> subfields) {
            if (subfields == null) {
                return;
            }
            for (Map.Entry<String, Object> entry : subfields.entrySet()) {
                script.emit(entry.getKey(), entry.getValue());
            }
        }
    }
}
