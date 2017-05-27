/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plan.logical;

import java.util.List;

import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.tree.Location;

import static java.util.Collections.emptyList;

/**
 * Special class when using SELECT without a FROM.
 * Rarely the case however it can be used be used as an alternative to the 'DUAL' table in Orace.
 */
public class FromlessSelect extends LeafPlan {

    public FromlessSelect(Location location) {
        super(location);
    }

    @Override
    public List<Attribute> output() {
        return emptyList();
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    @Override
    public int hashCode() {
        return FromlessSelect.class.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        return true;
    }
}
