/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.predicate;

import java.util.List;

/** Columns the residual predicate will load. The source prepares only these. */
public record Projection(List<FieldRef> fields) {
    public Projection {
        fields = List.copyOf(fields);
    }
}
