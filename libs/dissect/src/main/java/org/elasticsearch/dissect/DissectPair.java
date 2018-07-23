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

package org.elasticsearch.dissect;

import java.util.Objects;

/**
 * A tuple class holds a {@link DissectKey} and an associated String. The value associated with the key may have different meanings
 * based on the context. For example, the associated value may be the delimiter associated with the key or the parsed value associated with
 * the key.
 */
public final class DissectPair implements Comparable<DissectPair> {

    private final DissectKey key;
    private final String value;

    DissectPair(DissectKey key, String value) {
        this.key = key;
        this.value = value;
    }

    public DissectKey getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    //generated
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DissectPair that = (DissectPair) o;
        return Objects.equals(key, that.key) &&
            Objects.equals(value, that.value);
    }

    //generated
    @Override
    public int hashCode() {
        return Objects.hash(key, value);
    }

    //generated
    @Override
    public String toString() {
        return "DissectPair{" +
            "key=" + key +
            ", value='" + value + '\'' +
            '}';
    }

    @Override
    public int compareTo(DissectPair o) {
        if(this.getKey().getModifier().equals(DissectKey.Modifier.FIELD_NAME)){
            return -1;
        }
        if(this.getKey().getModifier().equals(DissectKey.Modifier.FIELD_VALUE)){
            return 1;
        }
        return Integer.compare(this.getKey().getOrderPosition(), o.getKey().getOrderPosition());
    }
}
