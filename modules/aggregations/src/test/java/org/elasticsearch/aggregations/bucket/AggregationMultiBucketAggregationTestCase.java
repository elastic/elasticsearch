/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.aggregations.bucket;

import org.elasticsearch.aggregations.AggregationsPlugin;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.test.InternalMultiBucketAggregationTestCase;
import org.elasticsearch.xcontent.ContextParser;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;

import java.util.List;
import java.util.Map;

/**
 * Base class for unit testing multi bucket aggregation's bucket implementations that reside in aggregations module.
 *
 * @param <T> The bucket type
 */
public abstract class AggregationMultiBucketAggregationTestCase<T extends InternalAggregation & MultiBucketsAggregation> extends
    InternalMultiBucketAggregationTestCase<T> {

    @Override
    protected SearchPlugin registerPlugin() {
        return new AggregationsPlugin();
    }

    @Override
    protected List<NamedXContentRegistry.Entry> getNamedXContents() {
        var entry = getParser();
        return CollectionUtils.appendToCopy(
            getDefaultNamedXContents(),
            new NamedXContentRegistry.Entry(Aggregation.class, new ParseField(entry.getKey()), entry.getValue())
        );
    }

    protected abstract Map.Entry<String, ContextParser<Object, Aggregation>> getParser();

}
