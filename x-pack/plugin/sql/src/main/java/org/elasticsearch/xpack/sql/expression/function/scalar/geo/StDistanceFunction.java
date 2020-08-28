/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.geo;

import org.elasticsearch.xpack.ql.expression.predicate.PredicateBiFunction;

class StDistanceFunction  implements PredicateBiFunction<Object, Object, Double> {

    @Override
    public String name() {
        return "ST_DISTANCE";
    }

    @Override
    public String symbol() {
        return "ST_DISTANCE";
    }

    @Override
    public Double doApply(Object s1, Object s2) {
        return StDistanceProcessor.process(s1, s2);
    }
}
