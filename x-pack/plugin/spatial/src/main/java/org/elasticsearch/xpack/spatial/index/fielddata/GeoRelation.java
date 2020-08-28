/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

/**
 * Enum for capturing relationships between a shape
 * and a query
 */
public enum GeoRelation {
    QUERY_CROSSES,
    QUERY_INSIDE,
    QUERY_DISJOINT
}
