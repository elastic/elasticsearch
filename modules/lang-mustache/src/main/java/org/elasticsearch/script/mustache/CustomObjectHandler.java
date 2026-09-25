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
import com.github.mustachejava.TemplateContext;
import com.github.mustachejava.codes.ValueCode;
import com.github.mustachejava.reflect.AbstractObjectHandler;
import com.github.mustachejava.util.Wrapper;

import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.iterable.Iterables;

import java.lang.reflect.Array;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

final class CustomObjectHandler extends AbstractObjectHandler {

    // Intentionally shadows AbstractObjectHandler.NOT_FOUND. We use our own sentinel to keep this
    // class's resolution logic independent of library internals, which could change across versions.
    private static final Object NOT_FOUND = new Object();

    private final boolean detectMissingParams;

    CustomObjectHandler(boolean detectMissingParams) {
        this.detectMissingParams = detectMissingParams;
    }

    @Override
    public Object coerce(Object object) {
        if (object == null) {
            return null;
        } else if (object.getClass().isArray()) {
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
        if (detectMissingParams) {
            return new DetectMissingParamsDirectBinding(name, code);
        }
        return new DirectMapBinding(name);
    }

    @SuppressWarnings("unchecked")
    private static Object mapGet(Object map, String key) {
        return ((Map<Object, Object>) map).getOrDefault(key, NOT_FOUND);
    }

    /**
     * Never called — both binding implementations resolve values directly without going through
     * {@code find()}, so this method is unreachable in normal operation.
     */
    @Override
    public Wrapper find(String name, List<Object> scopes) {
        throw new UnsupportedOperationException("find() is not used by this object handler");
    }

    /**
     * A {@link Binding} that bypasses mustache.java's guard/reflection machinery entirely.
     * <p>
     * The standard {@code GuardedBinding} caches a {@code ReflectionWrapper} per scope-type
     * signature and re-checks a set of type guards on every call to confirm the scope types
     * haven't changed before dispatching via reflection. For ingest templates the model is always
     * a {@code Map<String, Object>}, so the guards always pass and reflection always resolves to
     * {@code Map.get} — the guard loop and reflective dispatch are pure overhead.
     * <p>
     * This binding skips all of that: it searches the scope stack right-to-left and resolves
     * dot-separated components iteratively, using {@link #coerce} at each step so that arrays
     * and collections are wrapped as {@code ArrayMap}/{@code CollectionMap} for index access.
     */
    private final class DirectMapBinding implements Binding {
        private final String name;

        DirectMapBinding(String name) {
            this.name = name;
        }

        @Override
        public Object get(List<Object> scopes) {
            int dot = name.indexOf('.');
            // If the name contains dots, first try it as a literal key. This handles flat dotted
            // keys (e.g. "metadata.extra_group") stored without nesting in the scope map — as
            // used by the security role-mapping model.
            if (dot != -1) {
                for (int i = scopes.size() - 1; i >= 0; i--) {
                    Object scope = coerce(scopes.get(i));
                    if (scope instanceof Map) {
                        Object found = mapGet(scope, name);
                        if (found != NOT_FOUND) {
                            return coerce(found);
                        }
                    }
                }
            }
            String first = dot == -1 ? name : name.substring(0, dot);
            // Search scope stack right-to-left (innermost scope first) for the first component
            Object value = NOT_FOUND;
            for (int i = scopes.size() - 1; i >= 0; i--) {
                Object scope = coerce(scopes.get(i));
                if (scope instanceof Map) {
                    Object found = mapGet(scope, first);
                    if (found != NOT_FOUND) {
                        value = found;
                        break;
                    }
                }
            }
            if (dot == -1) {
                return value == NOT_FOUND ? null : coerce(value);
            }
            // Resolve remaining dot-separated components through coerce so that arrays and
            // collections are accessible by index via ArrayMap/CollectionMap. Malformed names with
            // empty segments (leading/trailing/consecutive dots) produce an empty-string part that
            // will not match any key, so the path silently resolves to null.
            for (String part : name.substring(dot + 1).split("\\.")) {
                Object coerced = value == NOT_FOUND ? null : coerce(value);
                if (coerced instanceof Map == false) {
                    return null;
                }
                value = mapGet(coerced, part);
            }
            return value == NOT_FOUND ? null : coerce(value);
        }
    }

    /**
     * A {@link Binding} that behaves like {@link DirectMapBinding} but additionally throws
     * {@link MustacheInvalidParameterException} when a {@code {{variable}}} substitution's
     * first path component is not found in any scope.
     * <p>
     * Silently returns {@code null} for section codes (non-{@code ValueCode}) so that missing
     * section variables are treated as falsey rather than as errors.
     */
    private final class DetectMissingParamsDirectBinding implements Binding {
        private final String name;
        private final boolean throwOnMissing;

        DetectMissingParamsDirectBinding(String name, Code code) {
            this.name = name;
            this.throwOnMissing = code instanceof ValueCode;
        }

        @Override
        public Object get(List<Object> scopes) {
            int dot = name.indexOf('.');
            // If the name contains dots, first try it as a literal key. This handles flat dotted
            // keys (e.g. "metadata.extra_group") stored without nesting in the scope map — as
            // used by the security role-mapping model.
            if (dot != -1) {
                for (int i = scopes.size() - 1; i >= 0; i--) {
                    Object scope = coerce(scopes.get(i));
                    if (scope instanceof Map) {
                        Object found = mapGet(scope, name);
                        if (found != NOT_FOUND) {
                            return coerce(found);
                        }
                    }
                }
            }
            String first = dot == -1 ? name : name.substring(0, dot);
            // Search scope stack right-to-left (innermost scope first) for the first component,
            // using NOT_FOUND to distinguish a present-but-null value from an absent key.
            Object value = NOT_FOUND;
            for (int i = scopes.size() - 1; i >= 0; i--) {
                Object scope = coerce(scopes.get(i));
                if (scope instanceof Map) {
                    Object found = mapGet(scope, first);
                    if (found != NOT_FOUND) {
                        value = found;
                        break;
                    }
                }
            }
            if (value == NOT_FOUND) {
                if (throwOnMissing) {
                    throw new MustacheInvalidParameterException("Parameter [" + name + "] is missing");
                }
                return null;
            }
            if (dot == -1) {
                return coerce(value);
            }
            // Resolve remaining dot-separated components, throwing on any missing component.
            // Malformed names with empty segments (leading/trailing/consecutive dots) produce an
            // empty-string part that will not match any key and throws MustacheInvalidParameterException.
            for (String part : name.substring(dot + 1).split("\\.")) {
                Object coerced = coerce(value);
                if (coerced instanceof Map == false) {
                    if (throwOnMissing) {
                        throw new MustacheInvalidParameterException("Parameter [" + name + "] is missing");
                    }
                    return null;
                }
                Object found = mapGet(coerced, part);
                if (found == NOT_FOUND) {
                    if (throwOnMissing) {
                        throw new MustacheInvalidParameterException("Parameter [" + name + "] is missing");
                    }
                    return null;
                }
                value = found;
            }
            return coerce(value);
        }
    }

    private static final class ArrayMap extends AbstractMap<Object, Object> implements Iterable<Object> {

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
            return new Iterator<>() {

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

    private static final class CollectionMap extends AbstractMap<Object, Object> implements Iterable<Object> {

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
        if (object instanceof String string) {
            return string; // if object is already a string, we can just return it
        } else {
            CollectionUtils.ensureNoSelfReferences(object, "CustomReflectionObjectHandler stringify");
            return super.stringify(object);
        }
    }
}
