/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.aggregate;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.tree.Location;

public class Stats extends CompoundAggregate {

    public Stats(Location location, Expression argument) {
        super(location, argument);
    }

    public static boolean isTypeCompatible(Expression e) {
        return e instanceof Min || e instanceof Max || e instanceof Avg || e instanceof Sum;
    }
}
