/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plan.logical.command;

import org.elasticsearch.xpack.sql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.sql.session.Executable;
import org.elasticsearch.xpack.sql.tree.Location;

import static java.util.Collections.emptyList;

import java.util.List;

public abstract class Command extends LogicalPlan implements Executable {

    public Command(Location location) {
        super(location, emptyList());
    }

    @Override
    public final LogicalPlan replaceChildren(List<LogicalPlan> newChildren) {
        throw new UnsupportedOperationException("this type of node doesn't have any children to replace");
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }
}
