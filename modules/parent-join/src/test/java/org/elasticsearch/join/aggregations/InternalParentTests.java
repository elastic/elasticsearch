/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.join.aggregations;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.NamedXContentRegistry.Entry;
import org.elasticsearch.join.ParentJoinPlugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalSingleBucketAggregationTestCase;
import org.elasticsearch.search.aggregations.bucket.ParsedSingleBucketAggregation;

import java.util.List;
import java.util.Map;

public class InternalParentTests extends InternalSingleBucketAggregationTestCase<InternalParent> {

    @Override
    protected SearchPlugin registerPlugin() {
        return new ParentJoinPlugin();
    }

    @Override
    protected List<Entry> getNamedXContents() {
        return CollectionUtils.appendToCopy(super.getNamedXContents(), new Entry(
                Aggregation.class, new ParseField(ParentAggregationBuilder.NAME), (p, c) -> ParsedParent.fromXContent(p, (String) c)));
    }

    @Override
    protected InternalParent createTestInstance(String name, long docCount, InternalAggregations aggregations,
            Map<String, Object> metadata) {
        return new InternalParent(name, docCount, aggregations, metadata);
    }

    @Override
    protected void extraAssertReduced(InternalParent reduced, List<InternalParent> inputs) {
        // Nothing extra to assert
    }

    @Override
    protected Class<? extends ParsedSingleBucketAggregation> implementationClass() {
        return ParsedParent.class;
    }
}
