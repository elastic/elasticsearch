/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression;

import java.util.Objects;

import org.elasticsearch.xpack.sql.capabilities.UnresolvedException;
import org.elasticsearch.xpack.sql.tree.Location;

import static java.util.Collections.emptyList;

public class UnresolvedStar extends UnresolvedNamedExpression {

    // typically used for nested fields
    private final UnresolvedAttribute qualifier;

    public UnresolvedStar(Location location, UnresolvedAttribute qualifier) {
        super(location, emptyList());
        this.qualifier = qualifier;
    }

    @Override
    public boolean nullable() {
        throw new UnresolvedException("nullable", this);
    }

    public UnresolvedAttribute qualifier() {
        return qualifier;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        UnresolvedStar other = (UnresolvedStar) obj;
        return Objects.equals(qualifier, other.qualifier);
    }

    @Override
    public String toString() {
        return UNRESOLVED_PREFIX + (qualifier() != null ? qualifier() + "." : "") + "*";
    }
}
