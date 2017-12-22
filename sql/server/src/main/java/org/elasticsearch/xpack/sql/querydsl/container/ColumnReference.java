/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.container;

import org.elasticsearch.xpack.sql.execution.search.FieldExtraction;
import org.elasticsearch.xpack.sql.execution.search.SqlSourceBuilder;

/**
 * Entity representing a 'column' backed by one or multiple results from ES. A
 * column reference can also extract a field (meta or otherwise) from a result
 * set, so extends {@link FieldExtraction}.
 */
public interface ColumnReference extends FieldExtraction {

    /**
     * Indicates the depth of the result. Used for counting the actual size of a
     * result by knowing how many nested levels there are. Typically used by
     * aggregations.
     * 
     * @return depth of the result
     */
    int depth();
}
