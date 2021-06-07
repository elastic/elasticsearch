/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.join.aggregations;

public abstract class JoinAggregationBuilders {
    /**
     * Create a new {@link Children} aggregation with the given name.
     */
    public static ChildrenAggregationBuilder children(String name, String childType) {
        return new ChildrenAggregationBuilder(name, childType);
    }

    /**
     * Create a new {@link Parent} aggregation with the given name.
     */
    public static ParentAggregationBuilder parent(String name, String childType) {
        return new ParentAggregationBuilder(name, childType);
    }
}
