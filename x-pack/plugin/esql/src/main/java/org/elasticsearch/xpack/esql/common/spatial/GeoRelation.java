/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.common.spatial;

/**
 * Enum for capturing relationships between a shape and a query.
 * Copied from {@code GeoRelation} in the spatial module.
 */
public enum GeoRelation {
    QUERY_CROSSES,
    QUERY_INSIDE,
    QUERY_DISJOINT,
    QUERY_CONTAINS
}
