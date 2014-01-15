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
import org.mozilla.javascript.Undefined;
import org.mozilla.javascript.Wrapper;

import java.util.List;

/**
 *
 */
public class NativeList implements Scriptable, Wrapper {
    private static final long serialVersionUID = 3664761893203964569L;

    private List<Object> list;
    private Scriptable parentScope;
    private Scriptable prototype;


    public static NativeList wrap(Scriptable scope, List<Object> list) {
        return new NativeList(scope, list);
    }

    public NativeList(Scriptable scope, List<Object> list) {
        this.parentScope = scope;
        this.list = list;
    }

    /* (non-Javadoc)
     * @see org.mozilla.javascript.Wrapper#unwrap()
     */

    public Object unwrap() {
        return list;
    }

    /* (non-Javadoc)
     * @see org.mozilla.javascript.Scriptable#getClassName()
     */

    public String getClassName() {
        return "NativeList";
    }

    /* (non-Javadoc)
     * @see org.mozilla.javascript.Scriptable#get(java.lang.String, org.mozilla.javascript.Scriptable)
     */

    public Object get(String name, Scriptable start) {
        if ("length".equals(name)) {
            return list.size();
        } else {
            return Undefined.instance;
        }
    }

    /* (non-Javadoc)
     * @see org.mozilla.javascript.Scriptable#get(int, org.mozilla.javascript.Scriptable)
     */

    public Object get(int index, Scriptable start) {
        if (index < 0 || index >= list.size()) {
            return Undefined.instance;
        }
        return list.get(index);
    }

    /* (non-Javadoc)
     * @see org.mozilla.javascript.Scriptable#has(java.lang.String, org.mozilla.javascript.Scriptable)
     */

    public boolean has(String name, Scriptable start) {
        if ("length".equals(name)) {
            return true;
        }
        return false;
    }

    /* (non-Javadoc)
     * @see org.mozilla.javascript.Scriptable#has(int, org.mozilla.javascript.Scriptable)
     */

    public boolean has(int index, Scriptable start) {
        return index >= 0 && index < list.size();
    }

    /* (non-Javadoc)
     * @see org.mozilla.javascript.Scriptable#put(java.lang.String, org.mozilla.javascript.Scriptable, java.lang.Object)
     */

    @SuppressWarnings("unchecked")
    public void put(String name, Scriptable start, Object value) {
        // do nothing here...
    }

    /* (non-Javadoc)
     * @see org.mozilla.javascript.Scriptable#put(int, org.mozilla.javascript.Scriptable, java.lang.Object)
     */

    public void put(int index, Scriptable start, Object value) {
        if (index == list.size()) {
            list.add(value);
        } else {
            list.set(index, value);
        }
    }

    /* (non-Javadoc)
     * @see org.mozilla.javascript.Scriptable#delete(java.lang.String)
     */

    public void delete(String name) {
        // nothing here
    }

    /* (non-Javadoc)
     * @see org.mozilla.javascript.Scriptable#delete(int)
     */

    public void delete(int index) {
        list.remove(index);
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
        int size = list.size();
        Object[] ids = new Object[size];
        for (int i = 0; i < size; ++i) {
            ids[i] = i;
        }
        return ids;
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
        return List.class.isInstance(instance);
    }

}
