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

package org.elasticsearch.search.slice;

import org.apache.lucene.search.Query;

import java.util.Objects;

/**
 * An abstract {@link Query} that defines an hash function to partition the documents in multiple slices.
 */
public abstract class SliceQuery extends Query {
    private final String field;
    private final int id;
    private final int max;

    /**
     * @param field The name of the field
     * @param id    The id of the slice
     * @param max   The maximum number of slices
     */
    public SliceQuery(String field, int id, int max) {
        this.field = field;
        this.id = id;
        this.max = max;
    }

    // Returns true if the value matches the predicate
    protected final boolean contains(long value) {
        return Math.floorMod(value, max) == id;
    }

    public String getField() {
        return field;
    }

    public int getId() {
        return id;
    }

    public int getMax() {
        return max;
    }

    @Override
    public boolean equals(Object o) {
        if (sameClassAs(o) == false) {
            return false;
        }
        SliceQuery that = (SliceQuery) o;
        return field.equals(that.field) && id == that.id && max == that.max;
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), field, id, max);
    }

    @Override
    public String toString(String f) {
        return getClass().getSimpleName() + "[field=" + field + ", id=" + id + ", max=" + max + "]";
    }

}
