/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.aggregate;

// Agg 'enclosed' by another agg. Used for agg that return multiple embedded aggs (like MatrixStats)
public interface EnclosedAgg {

    String innerName();
}
