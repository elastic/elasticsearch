/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.TemplateScript;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.script.Script.DEFAULT_TEMPLATE_LANG;

/**
 * Holds a value. If the value is requested a copy is made and optionally template snippets are resolved too.
 */
public interface ValueSource {

    /**
     * Returns a copy of the value this ValueSource holds and resolves templates if there're any.
     *
     * For immutable values only a copy of the reference to the value is made.
     *
     * @param model The model to be used when resolving any templates
     * @return copy of the wrapped value
     */
    Object copyAndResolve(Map<String, Object> model);

    static ValueSource wrap(Object value, ScriptService scriptService) {
        return wrap(value, scriptService, Map.of());
    }

    static ValueSource wrap(Object value, ScriptService scriptService, Map<String, String> scriptOptions) {

        if (value instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<Object, Object> mapValue = (Map) value;
            Map<ValueSource, ValueSource> valueTypeMap = Maps.newMapWithExpectedSize(mapValue.size());
            for (Map.Entry<Object, Object> entry : mapValue.entrySet()) {
                valueTypeMap.put(wrap(entry.getKey(), scriptService, scriptOptions), wrap(entry.getValue(), scriptService, scriptOptions));
            }
            return new MapValue(valueTypeMap);
        } else if (value instanceof List) {
            @SuppressWarnings("unchecked")
            List<Object> listValue = (List) value;
            List<ValueSource> valueSourceList = new ArrayList<>(listValue.size());
            for (Object item : listValue) {
                valueSourceList.add(wrap(item, scriptService, scriptOptions));
            }
            return new ListValue(valueSourceList);
        } else if (value == null || value instanceof Number || value instanceof Boolean) {
            return new ObjectValue(value);
        } else if (value instanceof byte[] bytes) {
            return new ByteValue(bytes);
        } else if (value instanceof String string) {
            // This check is here because the DEFAULT_TEMPLATE_LANG(mustache) is not
            // installed for use by REST tests. `value` will not be
            // modified if templating is not available
            if (scriptService.isLangSupported(DEFAULT_TEMPLATE_LANG) && string.contains("{{")) {
                Script script = new Script(ScriptType.INLINE, DEFAULT_TEMPLATE_LANG, string, scriptOptions, Map.of());
                return new TemplatedValue(scriptService.compile(script, TemplateScript.INGEST_CONTEXT));
            } else {
                return new ObjectValue(value);
            }
        } else {
            throw new IllegalArgumentException("unexpected value type [" + value.getClass() + "]");
        }
    }

    record MapValue(Map<ValueSource, ValueSource> map) implements ValueSource {

        @Override
        public Object copyAndResolve(Map<String, Object> model) {
            Map<Object, Object> copy = new HashMap<>();
            for (Map.Entry<ValueSource, ValueSource> entry : this.map.entrySet()) {
                copy.put(entry.getKey().copyAndResolve(model), entry.getValue().copyAndResolve(model));
            }
            return copy;
        }
    }

    record ListValue(List<ValueSource> values) implements ValueSource {

        @Override
        public Object copyAndResolve(Map<String, Object> model) {
            List<Object> copy = new ArrayList<>(values.size());
            for (ValueSource value : values) {
                copy.add(value.copyAndResolve(model));
            }
            return copy;
        }
    }

    record ObjectValue(Object value) implements ValueSource {

        @Override
        public Object copyAndResolve(Map<String, Object> model) {
            return value;
        }
    }

    record ByteValue(byte[] value) implements ValueSource {

        @Override
        public Object copyAndResolve(Map<String, Object> model) {
            return value;
        }

    }

    record TemplatedValue(TemplateScript.Factory template) implements ValueSource {

        @Override
        public Object copyAndResolve(Map<String, Object> model) {
            return template.newInstance(model).execute();
        }
    }

}
