/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.container;

/**
 * Entity representing a 'column' backed by one or multiple results from ES.
 */
public interface ColumnReference {

    /**
     * Indicates the depth of the result. Used for counting the actual size of a
     * result by knowing how many nested levels there are. Typically used by
     * aggregations.
     * 
     * @return depth of the result
     */
    int depth();
}
