/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.slice;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;

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

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(field)) {
            visitor.visitLeaf(this);
        }
    }
}
