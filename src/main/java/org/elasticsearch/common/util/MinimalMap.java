/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.common.util;

import org.elasticsearch.ElasticSearchException;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public abstract class MinimalMap implements Map {

    public boolean isEmpty() {
        throw new ElasticSearchException("entrySet() not supported!");
    }

    public Object put(Object key, Object value) {
        throw new ElasticSearchException("put() not supported!");
    }

    public void putAll(Map m) {
        throw new ElasticSearchException("putAll() not supported!");
    }

    public Object remove(Object key) {
        throw new ElasticSearchException("remove() not supported!");
    }

    public void clear() {
        throw new ElasticSearchException("clear() not supported!");
    }

    public Set<String> keySet() {
        throw new ElasticSearchException("keySet() not supported!");
    }

    public Collection<Object> values() {
        throw new ElasticSearchException("values() not supported!");
    }

    public Set<Entry<String, Object>> entrySet() {
        throw new ElasticSearchException("entrySet() not supported!");
    }

    public boolean containsValue(Object value) {
        throw new ElasticSearchException("containsValue() not supported!");
    }

    public int size() {
        throw new ElasticSearchException("size() not supported!");
    }

}