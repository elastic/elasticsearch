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

import org.mozilla.javascript.Scriptable;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Implementation of a Scriptable Map. This is the best choice for maps that want to represent
 * JavaScript associative arrays - allowing access via key and integer index. It maintains and
 * respects insertion order of the elements and allows either string or integer keys.
 *
 *
 */
public class ScriptableLinkedHashMap<K, V> extends LinkedHashMap<K, V> implements ScriptableMap<K, V> {
    private Scriptable parentScope;
    private Scriptable prototype;


    public ScriptableLinkedHashMap() {
    }

    public ScriptableLinkedHashMap(int initialCapacity) {
        super(initialCapacity);
    }

    public ScriptableLinkedHashMap(Map<K, V> source) {
        super(source);
    }

    /**
     * @see org.mozilla.javascript.Scriptable#getClassName()
     */
    @Override
    public String getClassName() {
        return "ScriptableMap";
    }

    /**
     * @see org.mozilla.javascript.Scriptable#get(java.lang.String, org.mozilla.javascript.Scriptable)
     */
    @Override
    public Object get(String name, Scriptable start) {
        // get the property from the underlying QName map
        if ("length".equals(name)) {
            return this.size();
        } else {
            return get(name);
        }
    }

    /**
     * @see org.mozilla.javascript.Scriptable#get(int, org.mozilla.javascript.Scriptable)
     */
    @Override
    public Object get(int index, Scriptable start) {
        Object value = null;
        int i = 0;
        Iterator<V> itrValues = this.values().iterator();
        while (i++ <= index && itrValues.hasNext()) {
            value = itrValues.next();
        }
        return value;
    }

    /**
     * @see org.mozilla.javascript.Scriptable#has(java.lang.String, org.mozilla.javascript.Scriptable)
     */
    @Override
    public boolean has(String name, Scriptable start) {
        // locate the property in the underlying map
        return containsKey(name);
    }

    /**
     * @see org.mozilla.javascript.Scriptable#has(int, org.mozilla.javascript.Scriptable)
     */
    @Override
    public boolean has(int index, Scriptable start) {
        return (index >= 0 && this.values().size() > index);
    }

    /**
     * @see org.mozilla.javascript.Scriptable#put(java.lang.String, org.mozilla.javascript.Scriptable, java.lang.Object)
     */
    @Override
    @SuppressWarnings("unchecked")
    public void put(String name, Scriptable start, Object value) {
        // add the property to the underlying QName map
        put((K) name, (V) value);
    }

    /**
     * @see org.mozilla.javascript.Scriptable#put(int, org.mozilla.javascript.Scriptable, java.lang.Object)
     */
    @Override
    public void put(int index, Scriptable start, Object value) {
        // TODO: implement?
    }

    /**
     * @see org.mozilla.javascript.Scriptable#delete(java.lang.String)
     */
    @Override
    public void delete(String name) {
        // remove the property from the underlying QName map
        remove(name);
    }

    /**
     * @see org.mozilla.javascript.Scriptable#delete(int)
     */
    @Override
    public void delete(int index) {
        int i = 0;
        Iterator<K> itrKeys = this.keySet().iterator();
        while (i <= index && itrKeys.hasNext()) {
            Object key = itrKeys.next();
            if (i == index) {
                remove(key);
                break;
            }
        }
    }

    /**
     * @see org.mozilla.javascript.Scriptable#getPrototype()
     */
    @Override
    public Scriptable getPrototype() {
        return this.prototype;
    }

    /**
     * @see org.mozilla.javascript.Scriptable#setPrototype(org.mozilla.javascript.Scriptable)
     */
    @Override
    public void setPrototype(Scriptable prototype) {
        this.prototype = prototype;
    }

    /**
     * @see org.mozilla.javascript.Scriptable#getParentScope()
     */
    @Override
    public Scriptable getParentScope() {
        return this.parentScope;
    }

    /**
     * @see org.mozilla.javascript.Scriptable#setParentScope(org.mozilla.javascript.Scriptable)
     */
    @Override
    public void setParentScope(Scriptable parent) {
        this.parentScope = parent;
    }

    /**
     * @see org.mozilla.javascript.Scriptable#getIds()
     */
    @Override
    public Object[] getIds() {
        return keySet().toArray();
    }

    /**
     * @see org.mozilla.javascript.Scriptable#getDefaultValue(java.lang.Class)
     */
    @Override
    public Object getDefaultValue(Class<?> hint) {
        return null;
    }

    /**
     * @see org.mozilla.javascript.Scriptable#hasInstance(org.mozilla.javascript.Scriptable)
     */
    @Override
    public boolean hasInstance(Scriptable instance) {
        return instance instanceof ScriptableLinkedHashMap;
    }
}

