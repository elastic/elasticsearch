/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.elasticsearch.compute.ann.MvCombiner;

/**
 * This multivalue combiner supports combining boolean types, where any true value will result in a true result.
 */
public class AnyCombiner implements MvCombiner<Boolean> {
    @Override
    public Boolean initial() {
        return false;
    }

    @Override
    public Boolean combine(Boolean previous, Boolean value) {
        return previous || value;
    }
}
