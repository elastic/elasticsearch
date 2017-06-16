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

package org.elasticsearch.common.collect;

public class LongTuple<T> {

    public static <T> LongTuple<T> tuple(final T v1, final long v2) {
        return new LongTuple<>(v1, v2);
    }

    private final T v1;
    private final long v2;

    private LongTuple(final T v1, final long v2) {
        this.v1 = v1;
        this.v2 = v2;
    }

    public T v1() {
        return v1;
    }

    public long v2() {
        return v2;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LongTuple tuple = (LongTuple) o;

        return (v1 == null ? tuple.v1 == null : v1.equals(tuple.v1)) && (v2 == tuple.v2);
    }

    @Override
    public int hashCode() {
        int result = v1 != null ? v1.hashCode() : 0;
        result = 31 * result + Long.hashCode(v2);
        return result;
    }

    @Override
    public String toString() {
        return "Tuple [v1=" + v1 + ", v2=" + v2 + "]";
    }

}