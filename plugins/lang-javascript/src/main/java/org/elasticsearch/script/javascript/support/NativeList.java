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

import org.mozilla.javascript.NativeJavaObject;
import org.mozilla.javascript.Scriptable;
import org.mozilla.javascript.Undefined;
import org.mozilla.javascript.Wrapper;

import java.util.Arrays;
import java.util.List;

/**
 *
 */
public class NativeList extends NativeJavaObject implements Scriptable, Wrapper {
    private static final long serialVersionUID = 3664761893203964569L;
    private static final String LENGTH_PROPERTY = "length";

    private final List<Object> list;


    public static NativeList wrap(Scriptable scope, List<Object> list, Class<?> staticType) {
        return new NativeList(scope, list, staticType);
    }

    private NativeList(Scriptable scope, List<Object> list, Class<?> staticType) {
        super(scope, list, staticType);
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
        if (LENGTH_PROPERTY.equals(name)) {
            return list.size();
        } else {
            return super.get(name, start);
        }
    }

    /* (non-Javadoc)
     * @see org.mozilla.javascript.Scriptable#get(int, org.mozilla.javascript.Scriptable)
     */

    public Object get(int index, Scriptable start) {
        if (has(index, start) == false) {
            return Undefined.instance;
        }
        return list.get(index);
    }

    /* (non-Javadoc)
     * @see org.mozilla.javascript.Scriptable#has(java.lang.String, org.mozilla.javascript.Scriptable)
     */

    public boolean has(String name, Scriptable start) {
        return super.has(name, start) || LENGTH_PROPERTY.equals(name);
    }

    /* (non-Javadoc)
     * @see org.mozilla.javascript.Scriptable#has(int, org.mozilla.javascript.Scriptable)
     */

    public boolean has(int index, Scriptable start) {
        return index >= 0 && index < list.size();
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
     * @see org.mozilla.javascript.Scriptable#delete(int)
     */

    public void delete(int index) {
        list.remove(index);
    }

    /* (non-Javadoc)
     * @see org.mozilla.javascript.Scriptable#getIds()
     */

    public Object[] getIds() {
        final Object[] javaObjectIds = super.getIds();
        final int size = list.size();
        final Object[] ids = Arrays.copyOf(javaObjectIds, javaObjectIds.length + size);
        for (int i = 0; i < size; ++i) {
            ids[javaObjectIds.length + i] = i;
        }
        return ids;
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
