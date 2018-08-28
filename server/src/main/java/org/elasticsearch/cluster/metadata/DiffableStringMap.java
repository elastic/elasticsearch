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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * This is a {@code Map<String, String>} that implements AbstractDiffable so it
 * can be used for cluster state purposes
 */
public class DiffableStringMap extends AbstractDiffable<DiffableStringMap> implements Map<String, String> {

    private final Map<String, String> innerMap;

    DiffableStringMap(final Map<String, String> map) {
        this.innerMap = map;
    }

    @SuppressWarnings("unchecked")
    DiffableStringMap(final StreamInput in) throws IOException {
        this.innerMap = (Map<String, String>) (Map) in.readMap();
    }

    @Override
    public int size() {
        return innerMap.size();
    }

    @Override
    public boolean isEmpty() {
        return innerMap.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return innerMap.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return innerMap.containsValue(value);
    }

    @Override
    public String get(Object key) {
        return innerMap.get(key);
    }

    @Override
    public String put(String key, String value) {
        return innerMap.put(key, value);
    }

    @Override
    public String remove(Object key) {
        return innerMap.remove(key);
    }

    @Override
    public void putAll(Map<? extends String, ? extends String> m) {
        innerMap.putAll(m);
    }

    @Override
    public void clear() {
        this.innerMap.clear();
    }

    @Override
    public Set<String> keySet() {
        return innerMap.keySet();
    }

    @Override
    public Collection<String> values() {
        return innerMap.values();
    }

    @Override
    public Set<Entry<String, String>> entrySet() {
        return innerMap.entrySet();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap((Map<String, Object>) (Map) innerMap);
    }

    @Override
    public Diff<DiffableStringMap> diff(DiffableStringMap previousState) {
        // TODO: implement this
        return super.diff(previousState);
    }

    public static Diff<DiffableStringMap> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(DiffableStringMap::new, in);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != this.getClass()) {
            return false;
        }
        DiffableStringMap other = (DiffableStringMap) obj;
        return innerMap.equals(other.innerMap);
    }

    @Override
    public int hashCode() {
        return innerMap.hashCode();
    }
}
