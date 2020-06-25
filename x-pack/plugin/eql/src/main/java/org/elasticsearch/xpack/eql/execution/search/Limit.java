/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.search;

import org.elasticsearch.xpack.eql.util.MathUtils;

import java.util.Objects;

public class Limit {

    public final int limit;
    public final int offset;
    public final int total;

    public Limit(int limit, int offset) {
        this.limit = limit;
        this.offset = offset;
        this.total = MathUtils.abs(limit) + offset;
    }

    public int absLimit() {
        return MathUtils.abs(limit);
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
}
