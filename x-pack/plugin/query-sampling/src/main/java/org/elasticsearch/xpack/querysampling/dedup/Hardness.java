/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.dedup;

/** How hard a query is to answer, in thirds of the queries of its field. */
public enum Hardness {
    EASY,
    MEDIUM,
    HARD
}
