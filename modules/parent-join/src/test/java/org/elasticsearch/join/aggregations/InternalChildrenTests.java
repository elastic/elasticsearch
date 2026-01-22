/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.join.aggregations;

import org.elasticsearch.join.ParentJoinPlugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalSingleBucketAggregationTestCase;

import java.util.List;
import java.util.Map;

public class InternalChildrenTests extends InternalSingleBucketAggregationTestCase<InternalChildren> {

    @Override
    protected SearchPlugin registerPlugin() {
        return new ParentJoinPlugin();
    }

    @Override
    protected InternalChildren createTestInstance(
        String name,
        long docCount,
        InternalAggregations aggregations,
        Map<String, Object> metadata
    ) {
        return new InternalChildren(name, docCount, aggregations, metadata);
    }

    @Override
    protected void extraAssertReduced(InternalChildren reduced, List<InternalChildren> inputs) {
        // Nothing extra to assert
    }
}
