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

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.MustacheException;
import com.github.mustachejava.reflect.ReflectionObjectHandler;
import com.google.common.collect.Iterables;

import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.Array;
import java.util.*;

/**
 * A custom MustacheFactory that does simple JSON escaping and supports array/collection access syntax in the form
 * of {@code array_or_collection.X} where {@code array_or_collection} refers to an array or a collection and
 * {@code X} is the index into it. For example, referring to the "key" field in the 3rd item in the "array" array
 * can be expressed as {@code array.3.key}.
 */
public final class InternalMustacheFactory extends DefaultMustacheFactory {

    public InternalMustacheFactory() {
        setObjectHandler(new InternalObjectHandler());
    }

    @Override
    public void encode(String value, Writer writer) {
        try {
            escape(value, writer);
        } catch (IOException e) {
            throw new MustacheException("Failed to encode value: " + value);
        }
    }

    public static Writer escape(String value, Writer writer) throws IOException {
        for (int i = 0; i < value.length(); i++) {
            final char character = value.charAt(i);
            if (isEscapeChar(character)) {
                writer.write('\\');
            }
            writer.write(character);
        }
        return writer;
    }

    public static boolean isEscapeChar(char c) {
        switch(c) {
            case '\b':
            case '\f':
            case '\n':
            case '\r':
            case '"':
            case '\\':
            case '\u000B': // vertical tab
            case '\t':
                return true;
        }
        return false;
    }

    /**
     * A new reflection based object handler for mustache that has special treatment for arrays, lists and sorted sets
     * in that, they can be treated as maps. This enables support for directly accessing elements in these data structures
     * based on the numeric index. For example, assuming {@code collection} refers to one of these data structures,
     * {@code collection.3} will refer to the 3rd element in it.
     *
     * Note that other collections (e.g. Sets) are not supported by this as their order is non-deterministic.
     */
    static class InternalObjectHandler extends ReflectionObjectHandler {

        @Override
        public Object coerce(Object object) {

            // we limit the index key access support to arrays, lists and sorted sets. We don't support
            // other collections as their order is non-deterministic, which may result with unexpected outputs
            if (object != null) {
                if (object.getClass().isArray()) {
                    return new ArrayMap(object);
                } else if (object instanceof List || object instanceof SortedSet) {
                    return new CollectionMap((Collection) object);
                }
            }
            return super.coerce(object);
        }

    }

    /**
     * A wrapper around an array object such that for mustache it will act as both an iterable object
     * and as a map. The keys of the map are numeric indices into the array. This enables accessing
     * elements in the array directly using expression such as {@code array.0}.
     */
    static class ArrayMap extends AbstractMap<Object, Object> implements Iterable<Object> {

        private final Object array;

        public ArrayMap(Object array) {
            this.array = array;
        }

        @Override
        public Object get(Object key) {
            if (key instanceof Number) {
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
            int length = Array.getLength(array);
            Map<Object, Object> map = new HashMap<>(length);
            for (int i = 0; i < length; i++) {
                map.put(i, Array.get(array, i));
            }
            return map.entrySet();
        }

        /**
         * Returns an iterator over a set of elements of type T.
         *
         * @return an Iterator.
         */
        @Override
        public Iterator<Object> iterator() {
            return new Iter(array);
        }

        static class Iter implements Iterator<Object> {

            private final Object array;
            private final int length;
            private int index;

            public Iter(Object array) {
                this.array = array;
                this.length = Array.getLength(array);
                this.index = 0;
            }

            @Override
            public boolean hasNext() {
                return index < length;
            }

            @Override
            public Object next() {
                return Array.get(array, index++);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("array iterator does not support removing elements");
            }
        }
    }

    /**
     * A wrapper around collection object such that for mustache it will act as both an iterable object
     * and as a map. The keys of the map are numeric indices into the array. This enables accessing
     * elements in the array directly using expression such as {@code array.0}.
     */
    static class CollectionMap extends AbstractMap<Object, Object> implements Iterable<Object> {

        private final Collection col;

        public CollectionMap(Collection col) {
            this.col = col;
        }

        @Override
        public Object get(Object key) {
            if (key instanceof Number) {
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

        /**
         * Returns an iterator over a set of elements of type T.
         *
         * @return an Iterator.
         */
        @Override
        public Iterator<Object> iterator() {
            return col.iterator();
        }
    }

}
