/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.predicate;

/** Three-valued evaluation over the candidates of a batch. Positions outside the mask are left false and are not read. */
public interface RowPredicate {
    PredicateResult evaluate(SelectionBatch batch, SelectionMask candidates);
}
