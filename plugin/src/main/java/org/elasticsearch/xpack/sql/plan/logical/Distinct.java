/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plan.logical;

import org.elasticsearch.xpack.sql.tree.Location;

public class Distinct extends UnaryPlan {

    public Distinct(Location location, LogicalPlan child) {
        super(location, child);
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }
}
