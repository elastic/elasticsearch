/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

/*
 * Written by Cliff Click and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */

package org.elasticsearch.util.concurrent.highscalelib;

import java.util.Map;

/**
 * A simple implementation of {@link java.util.Map.Entry}.
 * Does not implement {@link java.util.Map.Entry.setValue}, that is done by users of the class.
 *
 * @author Cliff Click
 * @param <TypeK> the type of keys maintained by this map
 * @param <TypeV> the type of mapped values
 * @since 1.5
 */

abstract class AbstractEntry<TypeK, TypeV> implements Map.Entry<TypeK, TypeV> {
    /**
     * Strongly typed key
     */
    protected final TypeK _key;
    /**
     * Strongly typed value
     */
    protected TypeV _val;

    public AbstractEntry(final TypeK key, final TypeV val) {
        _key = key;
        _val = val;
    }

    public AbstractEntry(final Map.Entry<TypeK, TypeV> e) {
        _key = e.getKey();
        _val = e.getValue();
    }

    /**
     * Return "key=val" string
     */
    public String toString() {
        return _key + "=" + _val;
    }

    /**
     * Return key
     */
    public TypeK getKey() {
        return _key;
    }

    /**
     * Return val
     */
    public TypeV getValue() {
        return _val;
    }

    /**
     * Equal if the underlying key & value are equal
     */
    public boolean equals(final Object o) {
        if (!(o instanceof Map.Entry)) return false;
        final Map.Entry e = (Map.Entry) o;
        return eq(_key, e.getKey()) && eq(_val, e.getValue());
    }

    /**
     * Compute <code>"key.hashCode() ^ val.hashCode()"</code>
     */
    public int hashCode() {
        return
                ((_key == null) ? 0 : _key.hashCode()) ^
                        ((_val == null) ? 0 : _val.hashCode());
    }

    private static boolean eq(final Object o1, final Object o2) {
        return (o1 == null ? o2 == null : o1.equals(o2));
    }
}

