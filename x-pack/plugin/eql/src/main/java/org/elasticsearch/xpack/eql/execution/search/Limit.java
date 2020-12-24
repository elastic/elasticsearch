/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.search;

import org.elasticsearch.xpack.eql.util.MathUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.emptyList;

public class Limit {

    private final int limit;
    private final int offset;
    private final int total;

    public Limit(int limit, int offset) {
        this.limit = limit;
        this.offset = offset;
        this.total = MathUtils.abs(limit) + offset;
    }

    public int limit() {
        return limit;
    }

    public int absLimit() {
        return MathUtils.abs(limit);
    }

    public int offset() {
        return offset;
    }

    public int totalLimit() {
        return total;
    }

    @Override
    public int hashCode() {
        return Objects.hash(limit, offset);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Limit other = (Limit) obj;
        return Objects.equals(limit, other.limit) && Objects.equals(offset, other.offset);
    }

    /**
     * Offer a limited view (including offset) for the given list.
     */
    public <E> List<E> view(List<E> values) {
        if (values == null || values.isEmpty()) {
            return values;
        }
        if (limit == 0) {
            return emptyList();
        }
        
        if (limit < 0) {
            values = new ArrayList<>(values);
            Collections.reverse(values);
        }
        int size = values.size();

        if (size >= total) {
            return values.subList(offset, total);
        }
        int l = absLimit();
        if (size <= l) {
            return values;
        }
        return values.subList(size - l, values.size());
    }
}