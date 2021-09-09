/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.TemplateScript;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
            Map<ValueSource, ValueSource> valueTypeMap = new HashMap<>(mapValue.size());
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
        } else if (value instanceof byte[]) {
            return new ByteValue((byte[]) value);
        } else if (value instanceof String) {
            // This check is here because the DEFAULT_TEMPLATE_LANG(mustache) is not
            // installed for use by REST tests. `value` will not be
            // modified if templating is not available
            if (scriptService.isLangSupported(DEFAULT_TEMPLATE_LANG) && ((String) value).contains("{{")) {
                Script script = new Script(ScriptType.INLINE, DEFAULT_TEMPLATE_LANG, (String) value, scriptOptions, Map.of());
                return new TemplatedValue(scriptService.compile(script, TemplateScript.INGEST_CONTEXT));
            } else {
                return new ObjectValue(value);
            }
        } else {
            throw new IllegalArgumentException("unexpected value type [" + value.getClass() + "]");
        }
    }

    final class MapValue implements ValueSource {

        private final Map<ValueSource, ValueSource> map;

        MapValue(Map<ValueSource, ValueSource> map) {
            this.map = map;
        }

        @Override
        public Object copyAndResolve(Map<String, Object> model) {
            Map<Object, Object> copy = new HashMap<>();
            for (Map.Entry<ValueSource, ValueSource> entry : this.map.entrySet()) {
                copy.put(entry.getKey().copyAndResolve(model), entry.getValue().copyAndResolve(model));
            }
            return copy;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            MapValue mapValue = (MapValue) o;
            return map.equals(mapValue.map);

        }

        @Override
        public int hashCode() {
            return map.hashCode();
        }
    }

    final class ListValue implements ValueSource {

        private final List<ValueSource> values;

        ListValue(List<ValueSource> values) {
            this.values = values;
        }

        @Override
        public Object copyAndResolve(Map<String, Object> model) {
            List<Object> copy = new ArrayList<>(values.size());
            for (ValueSource value : values) {
                copy.add(value.copyAndResolve(model));
            }
            return copy;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ListValue listValue = (ListValue) o;
            return values.equals(listValue.values);

        }

        @Override
        public int hashCode() {
            return values.hashCode();
        }
    }

    final class ObjectValue implements ValueSource {

        private final Object value;

        ObjectValue(Object value) {
            this.value = value;
        }

        @Override
        public Object copyAndResolve(Map<String, Object> model) {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ObjectValue objectValue = (ObjectValue) o;
            return Objects.equals(value, objectValue.value);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(value);
        }
    }

    final class ByteValue implements ValueSource {

        private final byte[] value;

        ByteValue(byte[] value) {
            this.value = value;
        }

        @Override
        public Object copyAndResolve(Map<String, Object> model) {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ByteValue objectValue = (ByteValue) o;
            return Arrays.equals(value, objectValue.value);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(value);
        }

    }

    final class TemplatedValue implements ValueSource {

        private final TemplateScript.Factory template;

        TemplatedValue(TemplateScript.Factory template) {
            this.template = template;
        }

        @Override
        public Object copyAndResolve(Map<String, Object> model) {
            return template.newInstance(model).execute();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TemplatedValue templatedValue = (TemplatedValue) o;
            return Objects.equals(template, templatedValue.template);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(template);
        }
    }

}
