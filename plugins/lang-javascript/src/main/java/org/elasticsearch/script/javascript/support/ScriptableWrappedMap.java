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

package org.elasticsearch.script.javascript.support;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.mozilla.javascript.Scriptable;
import org.mozilla.javascript.Wrapper;

/**
 * Implementation of a Scriptable Map. This is the best choice where you want values to be
 * persisted directly to an underlying map supplied on construction. The class automatically
 * wraps/unwraps JS objects as they enter/leave the underlying map via the Scriptable interface
 * methods - objects are untouched if accessed via the usual Map interface methods.
 * <p>Access should be by string key only - not integer index - unless you are sure the wrapped
 * map will maintain insertion order of the elements.
 *
 *
 */
public class ScriptableWrappedMap implements ScriptableMap<Object, Object>, Wrapper {
    private Map<Object, Object> map;
    private Scriptable parentScope;
    private Scriptable prototype;


    /**
     * Construction
     * @return scriptable wrapped map
     */
    public static ScriptableWrappedMap wrap(Scriptable scope, Map<Object, Object> map) {
        return new ScriptableWrappedMap(scope, map);
    }

    /**
     * Construct
     */
    public ScriptableWrappedMap(Map<Object, Object> map) {
        this.map = map;
    }

    /**
     * Construct
     */
    public ScriptableWrappedMap(Scriptable scope, Map<Object, Object> map) {
        this.parentScope = scope;
        this.map = map;
    }

    @Override
    public Object unwrap() {
        return map;
    }

    @Override
    public String getClassName() {
        return "ScriptableWrappedMap";
    }

    @Override
    public Object get(String name, Scriptable start) {
        // get the property from the underlying QName map
        if ("length".equals(name)) {
            return map.size();
        } else {
            return ScriptValueConverter.wrapValue(this.parentScope != null ? this.parentScope : start, map.get(name));
        }
    }

    @Override
    public Object get(int index, Scriptable start) {
        Object value = null;
        int i = 0;
        Iterator<Object> itrValues = map.values().iterator();
        while (i++ <= index && itrValues.hasNext()) {
            value = itrValues.next();
        }
        return ScriptValueConverter.wrapValue(this.parentScope != null ? this.parentScope : start, value);
    }

    @Override
    public boolean has(String name, Scriptable start) {
        // locate the property in the underlying map
        return map.containsKey(name);
    }

    @Override
    public boolean has(int index, Scriptable start) {
        return (index >= 0 && map.values().size() > index);
    }

    @Override
    public void put(String name, Scriptable start, Object value) {
        map.put(name, ScriptValueConverter.unwrapValue(value));
    }

    @Override
    public void put(int index, Scriptable start, Object value) {
        // TODO: implement?
    }

    @Override
    public void delete(String name) {
        map.remove(name);
    }

    @Override
    public void delete(int index) {
        int i = 0;
        Iterator<Object> itrKeys = map.keySet().iterator();
        while (i <= index && itrKeys.hasNext()) {
            Object key = itrKeys.next();
            if (i == index) {
                map.remove(key);
                break;
            }
        }
    }

    @Override
    public Scriptable getPrototype() {
        return this.prototype;
    }

    @Override
    public void setPrototype(Scriptable prototype) {
        this.prototype = prototype;
    }

    @Override
    public Scriptable getParentScope() {
        return this.parentScope;
    }

    @Override
    public void setParentScope(Scriptable parent) {
        this.parentScope = parent;
    }

    @Override
    public Object[] getIds() {
        return map.keySet().toArray();
    }

    @Override
    public Object getDefaultValue(Class<?> hint) {
        return null;
    }

    @Override
    public boolean hasInstance(Scriptable value) {
        if (!(value instanceof Wrapper))
            return false;
        Object instance = ((Wrapper) value).unwrap();
        return Map.class.isInstance(instance);
    }

    @Override
    public void clear() {
        this.map.clear();
    }

    @Override
    public boolean containsKey(Object key) {
        return this.map.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return this.map.containsValue(value);
    }

    @Override
    public Set<Map.Entry<Object, Object>> entrySet() {
        return this.map.entrySet();
    }

    @Override
    public Object get(Object key) {
        return this.map.get(key);
    }

    @Override
    public boolean isEmpty() {
        return (this.map.size() == 0);
    }

    @Override
    public Set<Object> keySet() {
        return this.map.keySet();
    }

    @Override
    public Object put(Object key, Object value) {
        return this.map.put(key, value);
    }

    @Override
    public void putAll(Map<? extends Object, ? extends Object> t) {
        this.map.putAll(t);
    }

    @Override
    public Object remove(Object key) {
        return this.map.remove(key);
    }

    @Override
    public int size() {
        return this.map.size();
    }

    @Override
    public Collection<Object> values() {
        return this.map.values();
    }

    @Override
    public String toString() {
        return (this.map != null ? this.map.toString() : super.toString());
    }
}
