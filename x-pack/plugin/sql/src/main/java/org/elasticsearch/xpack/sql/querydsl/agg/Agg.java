/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.agg;

import java.util.Locale;
import java.util.Objects;

import static java.lang.String.format;

/**
 * Base class holding common properties for Elasticsearch aggregations.
 */
public abstract class Agg {

    private final String id;
    private final AggTarget target;

    Agg(String id, AggTarget target) {
        this.id = id;
        Objects.requireNonNull(target, "AggTarget must not be null");
        this.target = target;
    }

    public String id() {
        return id;
    }

    public AggTarget target() {
        return target;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id) + target.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Agg other = (Agg) obj;
        return Objects.equals(id, other.id)
            && Objects.equals(target, other.target);
    }

    @Override
    public String toString() {
        return format(Locale.ROOT, "%s(%s)", getClass().getSimpleName(), target.toString());
    }
}
