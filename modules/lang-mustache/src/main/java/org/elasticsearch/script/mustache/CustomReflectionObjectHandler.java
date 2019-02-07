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

package org.elasticsearch.script.mustache;

import com.github.mustachejava.reflect.ReflectionObjectHandler;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.iterable.Iterables;

import java.lang.reflect.Array;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Set;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;

final class CustomReflectionObjectHandler extends ReflectionObjectHandler {

    @Override
    public Object coerce(Object object) {
        if (object == null) {
            return null;
        }

        if (object.getClass().isArray()) {
            return new ArrayMap(object);
        } else if (object instanceof Collection) {
            @SuppressWarnings("unchecked")
            Collection<Object> collection = (Collection<Object>) object;
            return new CollectionMap(collection);
        } else {
            return super.coerce(object);
        }
    }

    static final class ArrayMap extends AbstractMap<Object, Object> implements Iterable<Object> {

        private final Object array;
        private final int length;

        ArrayMap(Object array) {
            this.array = array;
            this.length = Array.getLength(array);
        }

        @Override
        public Object get(Object key) {
            if ("size".equals(key)) {
                return size();
            } else if (key instanceof Number) {
                return Array.get(array, ((Number) key).intValue());
            }
            try {
                int index = Integer.parseInt(key.toString());
                return Array.get(array, index);
            } catch (NumberFormatException nfe) {
                // if it's not a number it is as if the key doesn't exist
                return null;
            }
        }

        @Override
        public boolean containsKey(Object key) {
            return get(key) != null;
        }

        @Override
        public Set<Entry<Object, Object>> entrySet() {
            Map<Object, Object> map = new HashMap<>(length);
            for (int i = 0; i < length; i++) {
                map.put(i, Array.get(array, i));
            }
            return map.entrySet();
        }

        @Override
        public Iterator<Object> iterator() {
            return new Iterator<Object>() {

                int index = 0;

                @Override
                public boolean hasNext() {
                    return index < length;
                }

                @Override
                public Object next() {
                    return Array.get(array, index++);
                }
            };
        }

    }

    static final class CollectionMap extends AbstractMap<Object, Object> implements Iterable<Object> {

        private final Collection<Object> col;

        CollectionMap(Collection<Object> col) {
            this.col = col;
        }

        @Override
        public Object get(Object key) {
            if ("size".equals(key)) {
                return col.size();
            } else if (key instanceof Number) {
                return Iterables.get(col, ((Number) key).intValue());
            }
            try {
                int index = Integer.parseInt(key.toString());
                return Iterables.get(col, index);
            } catch (NumberFormatException nfe) {
                // if it's not a number it is as if the key doesn't exist
                return null;
            }
        }

        @Override
        public boolean containsKey(Object key) {
            return get(key) != null;
        }

        @Override
        public Set<Entry<Object, Object>> entrySet() {
            Map<Object, Object> map = new HashMap<>(col.size());
            int i = 0;
            for (Object item : col) {
                map.put(i++, item);
            }
            return map.entrySet();
        }

        @Override
        public Iterator<Object> iterator() {
            return col.iterator();
        }
    }

    @Override
    public String stringify(Object object) {
        CollectionUtils.ensureNoSelfReferences(object, "CustomReflectionObjectHandler stringify");
        return super.stringify(object);
    }
}
