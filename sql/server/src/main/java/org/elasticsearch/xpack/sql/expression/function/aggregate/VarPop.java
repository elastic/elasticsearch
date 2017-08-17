/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.aggregate;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.tree.Location;

public class VarPop extends NumericAggregate implements ExtendedStatsEnclosed {

    public VarPop(Location location, Expression field) {
        super(location, field);
    }

    @Override
    public String innerName() {
        return "variance";
    }
}
