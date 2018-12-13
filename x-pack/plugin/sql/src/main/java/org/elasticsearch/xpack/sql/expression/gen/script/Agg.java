/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.gen.script;

import org.elasticsearch.xpack.sql.expression.function.aggregate.AggregateFunctionAttribute;

class Agg extends Param<AggregateFunctionAttribute> {

    Agg(AggregateFunctionAttribute aggRef) {
        super(aggRef);
    }

    String aggName() {
        return value().functionId();
    }

    public String aggProperty() {
        return value().propertyPath();
    }

    @Override
    public String prefix() {
        return "a";
    }
}