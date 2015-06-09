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
import org.mozilla.javascript.Wrapper;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Implementation of a Scriptable Map. This is the best choice where you want values to be
 * persisted directly to an underlying map supplied on construction. The class automatically
 * wraps/unwraps JS objects as they enter/leave the underlying map via the Scriptable interface
 * methods - objects are untouched if accessed via the usual Map interface methods.
 * <p/>
 * <p>Access should be by string key only - not integer index - unless you are sure the wrapped
 * map will maintain insertion order of the elements.
 *
 *
 */
public class ScriptableWrappedMap implements ScriptableMap, Wrapper {
    private Map map;
    private Scriptable parentScope;
    private Scriptable prototype;


    /**
     * Construction
     *
     * @param scope
     * @param map
     * @return scriptable wrapped map
     */
    public static ScriptableWrappedMap wrap(Scriptable scope, Map<Object, Object> map) {
        return new ScriptableWrappedMap(scope, map);
    }

    /**
     * Construct
     *
     * @param map
     */
    public ScriptableWrappedMap(Map map) {
        this.map = map;
    }

    /**
     * Construct
     *
     * @param scope
     * @param map
     */
    public ScriptableWrappedMap(Scriptable scope, Map map) {
        this.parentScope = scope;
        this.map = map;
    }

    /* (non-Javadoc)
    * @see org.mozilla.javascript.Wrapper#unwrap()
    */

    public Object unwrap() {
        return map;
    }

    /* (non-Javadoc)
     * @see org.mozilla.javascript.Scriptable#getClassName()
     */

    public String getClassName() {
        return "ScriptableWrappedMap";
    }

    /* (non-Javadoc)
    * @see org.mozilla.javascript.Scriptable#get(java.lang.String, org.mozilla.javascript.Scriptable)
    */

    public Object get(String name, Scriptable start) {
        // get the property from the underlying QName map
        if ("length".equals(name)) {
            return map.size();
        } else {
            return ScriptValueConverter.wrapValue(this.parentScope != null ? this.parentScope : start, map.get(name));
        }
    }

    /* (non-Javadoc)
     * @see org.mozilla.javascript.Scriptable#get(int, org.mozilla.javascript.Scriptable)
     */

    public Object get(int index, Scriptable start) {
        Object value = null;
        int i = 0;
        Iterator itrValues = map.values().iterator();
        while (i++ <= index && itrValues.hasNext()) {
            value = itrValues.next();
        }
        return ScriptValueConverter.wrapValue(this.parentScope != null ? this.parentScope : start, value);
    }

    /* (non-Javadoc)
     * @see org.mozilla.javascript.Scriptable#has(java.lang.String, org.mozilla.javascript.Scriptable)
     */

    public boolean has(String name, Scriptable start) {
        // locate the property in the underlying map
        return map.containsKey(name);
    }

    /* (non-Javadoc)
     * @see org.mozilla.javascript.Scriptable#has(int, org.mozilla.javascript.Scriptable)
     */

    public boolean has(int index, Scriptable start) {
        return (index >= 0 && map.values().size() > index);
    }

    /* (non-Javadoc)
     * @see org.mozilla.javascript.Scriptable#put(java.lang.String, org.mozilla.javascript.Scriptable, java.lang.Object)
     */

    @SuppressWarnings("unchecked")
    public void put(String name, Scriptable start, Object value) {
        map.put(name, ScriptValueConverter.unwrapValue(value));
    }

    /* (non-Javadoc)
     * @see org.mozilla.javascript.Scriptable#put(int, org.mozilla.javascript.Scriptable, java.lang.Object)
     */

    public void put(int index, Scriptable start, Object value) {
        // TODO: implement?
    }

    /* (non-Javadoc)
     * @see org.mozilla.javascript.Scriptable#delete(java.lang.String)
     */

    public void delete(String name) {
        map.remove(name);
    }

    /* (non-Javadoc)
     * @see org.mozilla.javascript.Scriptable#delete(int)
     */

    public void delete(int index) {
        int i = 0;
        Iterator itrKeys = map.keySet().iterator();
        while (i <= index && itrKeys.hasNext()) {
            Object key = itrKeys.next();
            if (i == index) {
                map.remove(key);
                break;
            }
        }
    }

    /* (non-Javadoc)
     * @see org.mozilla.javascript.Scriptable#getPrototype()
     */

    public Scriptable getPrototype() {
        return this.prototype;
    }

    /* (non-Javadoc)
     * @see org.mozilla.javascript.Scriptable#setPrototype(org.mozilla.javascript.Scriptable)
     */

    public void setPrototype(Scriptable prototype) {
        this.prototype = prototype;
    }

    /* (non-Javadoc)
     * @see org.mozilla.javascript.Scriptable#getParentScope()
     */

    public Scriptable getParentScope() {
        return this.parentScope;
    }

    /* (non-Javadoc)
     * @see org.mozilla.javascript.Scriptable#setParentScope(org.mozilla.javascript.Scriptable)
     */

    public void setParentScope(Scriptable parent) {
        this.parentScope = parent;
    }

    /* (non-Javadoc)
     * @see org.mozilla.javascript.Scriptable#getIds()
     */

    public Object[] getIds() {
        return map.keySet().toArray();
    }

    /* (non-Javadoc)
     * @see org.mozilla.javascript.Scriptable#getDefaultValue(java.lang.Class)
     */

    public Object getDefaultValue(Class hint) {
        return null;
    }

    /* (non-Javadoc)
     * @see org.mozilla.javascript.Scriptable#hasInstance(org.mozilla.javascript.Scriptable)
     */

    public boolean hasInstance(Scriptable value) {
        if (!(value instanceof Wrapper))
            return false;
        Object instance = ((Wrapper) value).unwrap();
        return Map.class.isInstance(instance);
    }

    /* (non-Javadoc)
    * @see java.util.Map#clear()
    */

    public void clear() {
        this.map.clear();
    }

    /* (non-Javadoc)
     * @see java.util.Map#containsKey(java.lang.Object)
     */

    public boolean containsKey(Object key) {
        return this.map.containsKey(key);
    }

    /* (non-Javadoc)
     * @see java.util.Map#containsValue(java.lang.Object)
     */

    public boolean containsValue(Object value) {
        return this.map.containsValue(value);
    }

    /* (non-Javadoc)
     * @see java.util.Map#entrySet()
     */

    public Set entrySet() {
        return this.map.entrySet();
    }

    /* (non-Javadoc)
     * @see java.util.Map#get(java.lang.Object)
     */

    public Object get(Object key) {
        return this.map.get(key);
    }

    /* (non-Javadoc)
     * @see java.util.Map#isEmpty()
     */

    public boolean isEmpty() {
        return (this.map.size() == 0);
    }

    /* (non-Javadoc)
     * @see java.util.Map#keySet()
     */

    public Set keySet() {
        return this.map.keySet();
    }

    /* (non-Javadoc)
     * @see java.util.Map#put(java.lang.Object, java.lang.Object)
     */

    public Object put(Object key, Object value) {
        return this.map.put(key, value);
    }

    /* (non-Javadoc)
     * @see java.util.Map#putAll(java.util.Map)
     */

    public void putAll(Map t) {
        this.map.putAll(t);
    }

    /* (non-Javadoc)
     * @see java.util.Map#remove(java.lang.Object)
     */

    public Object remove(Object key) {
        return this.map.remove(key);
    }

    /* (non-Javadoc)
     * @see java.util.Map#size()
     */

    public int size() {
        return this.map.size();
    }

    /* (non-Javadoc)
     * @see java.util.Map#values()
     */

    public Collection values() {
        return this.map.values();
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */

    @Override
    public String toString() {
        return (this.map != null ? this.map.toString() : super.toString());
    }
}
