/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.vectors;

import org.elasticsearch.xcontent.ToXContent;

import java.util.Objects;

public abstract class IndexOptions implements ToXContent {
    protected final IndexType type;

    IndexOptions(IndexType type) {
        this.type = type;
    }

    public IndexType getType() {
        return type;
    }

    abstract boolean updatableTo(IndexOptions update);

    abstract boolean doEquals(IndexOptions other);

    abstract int doHashCode();

    @Override
    public final boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if (other == null || other.getClass() != getClass()) {
            return false;
        }
        IndexOptions otherOptions = (IndexOptions) other;
        return Objects.equals(type, otherOptions.type) && doEquals(otherOptions);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(type, doHashCode());
    }
}
