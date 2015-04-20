/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.template.xmustache;

import org.elasticsearch.common.collect.Iterables;
import org.elasticsearch.common.mustache.DefaultMustacheFactory;
import org.elasticsearch.common.mustache.MustacheException;
import org.elasticsearch.common.mustache.reflect.ReflectionObjectHandler;

import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.Array;
import java.util.*;

/**
 * An extension to elasticsearch's {@code JsonEscapingMustacheFactory} that on top of applying json
 * escapes it also enables support for navigating arrays using `array.X` notation (where `X` is the index
 * of the element in the array).
 */
public class XMustacheFactory extends DefaultMustacheFactory {

    public XMustacheFactory() {
        setObjectHandler(new ReflectionObjectHandler() {
            @Override
            public Object coerce(Object object) {
                if (object != null) {
                    if (object.getClass().isArray()) {
                        return new ArrayMap(object);
                    } else if (object instanceof Collection) {
                        return new CollectionMap((Collection) object);
                    }
                }
                return super.coerce(object);
            }
        });
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
        switch (c) {
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
