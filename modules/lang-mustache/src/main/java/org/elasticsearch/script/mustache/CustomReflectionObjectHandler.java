/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script.mustache;

import com.github.mustachejava.Binding;
import com.github.mustachejava.Code;
import com.github.mustachejava.ObjectHandler;
import com.github.mustachejava.TemplateContext;
import com.github.mustachejava.codes.ValueCode;
import com.github.mustachejava.reflect.GuardedBinding;
import com.github.mustachejava.reflect.MissingWrapper;
import com.github.mustachejava.reflect.ReflectionObjectHandler;
import com.github.mustachejava.util.Wrapper;

import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.iterable.Iterables;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Array;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

final class CustomReflectionObjectHandler extends ReflectionObjectHandler {
    private final boolean detectMissingParams;

    CustomReflectionObjectHandler(boolean detectMissingParams) {
        this.detectMissingParams = detectMissingParams;
    }

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

    @Override
    public Binding createBinding(String name, TemplateContext tc, Code code) {
        return detectMissingParams ? new DetectMissingParamsGuardedBinding(this, name, tc, code) : super.createBinding(name, tc, code);
    }

    @Override
    @SuppressWarnings("rawtypes")
    protected AccessibleObject findMember(Class sClass, String name) {
        /*
         * overriding findMember from BaseObjectHandler (our superclass's superclass) to always return null.
         *
         * if you trace findMember there, you'll see that it always either returns null or invokes the getMethod
         * or getField methods of that class. the last thing that getMethod and getField do is call 'setAccessible'
         * but we don't have java.lang.reflect.ReflectPermission/suppressAccessChecks so that will always throw an
         * exception.
         *
         * that is, with the permissions we're running with, it would always return null ('not found!') or throw
         * an exception ('found, but you cannot do this!') -- so by overriding to null we're effectively saying
         * "you will never find success going down this path, so don't bother trying"
         */
        return null;
    }

    static class DetectMissingParamsGuardedBinding extends GuardedBinding {
        private final Code code;

        DetectMissingParamsGuardedBinding(ObjectHandler oh, String name, TemplateContext tc, Code code) {
            super(oh, name, tc, code);
            this.code = code;
        }

        protected synchronized Wrapper getWrapper(String name, List<Object> scopes) {
            Wrapper wrapper = super.getWrapper(name, scopes);
            if (wrapper instanceof MissingWrapper && code instanceof ValueCode) {
                throw new MustacheInvalidParameterException("Parameter [" + name + "] is missing");
            }
            return wrapper;
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
            } else if (key instanceof Number number) {
                return number.intValue() >= 0 && number.intValue() < length ? Array.get(array, number.intValue()) : null;
            }
            try {
                int index = Integer.parseInt(key.toString());
                return index >= 0 && index < length ? Array.get(array, index) : null;
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
            Map<Object, Object> map = Maps.newMapWithExpectedSize(length);
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
            } else if (key instanceof Number number) {
                return number.intValue() >= 0 && number.intValue() < col.size() ? Iterables.get(col, number.intValue()) : null;
            }
            try {
                int index = Integer.parseInt(key.toString());
                return index >= 0 && index < col.size() ? Iterables.get(col, index) : null;
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
            Map<Object, Object> map = Maps.newMapWithExpectedSize(col.size());
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
