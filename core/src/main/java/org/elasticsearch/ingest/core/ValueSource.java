/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.ingest.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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

    static ValueSource wrap(Object value, TemplateService templateService) {
        if (value instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<Object, Object> mapValue = (Map) value;
            Map<ValueSource, ValueSource> valueTypeMap = new HashMap<>(mapValue.size());
            for (Map.Entry<Object, Object> entry : mapValue.entrySet()) {
                valueTypeMap.put(wrap(entry.getKey(), templateService), wrap(entry.getValue(), templateService));
            }
            return new MapValue(valueTypeMap);
        } else if (value instanceof List) {
            @SuppressWarnings("unchecked")
            List<Object> listValue = (List) value;
            List<ValueSource> valueSourceList = new ArrayList<>(listValue.size());
            for (Object item : listValue) {
                valueSourceList.add(wrap(item, templateService));
            }
            return new ListValue(valueSourceList);
        } else if (value == null || value instanceof Number || value instanceof Boolean) {
            return new ObjectValue(value);
        } else if (value instanceof byte[]) {
            return new ByteValue((byte[]) value);
        } else if (value instanceof String) {
            return new TemplatedValue(templateService.compile((String) value));
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

        private final TemplateService.Template template;

        TemplatedValue(TemplateService.Template template) {
            this.template = template;
        }

        @Override
        public Object copyAndResolve(Map<String, Object> model) {
            return template.execute(model);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TemplatedValue templatedValue = (TemplatedValue) o;
            return Objects.equals(template.getKey(), templatedValue.template.getKey());
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(template.getKey());
        }
    }

}
