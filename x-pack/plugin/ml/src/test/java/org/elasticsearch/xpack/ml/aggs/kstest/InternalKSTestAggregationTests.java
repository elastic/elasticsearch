/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.kstest;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

public class InternalKSTestAggregationTests extends InternalAggregationTestCase<InternalKSTestAggregation> {

    @Override
    protected SearchPlugin registerPlugin() {
        return new MachineLearning(Settings.EMPTY);
    }

    @Override
    protected List<NamedXContentRegistry.Entry> getNamedXContents() {
        return CollectionUtils.appendToCopy(
            super.getNamedXContents(),
            new NamedXContentRegistry.Entry(
                Aggregation.class,
                BucketCountKSTestAggregationBuilder.NAME,
                (p, c) -> ParsedKSTest.fromXContent(p, (String) c)
            )
        );
    }

    @Override
    protected InternalKSTestAggregation createTestInstance(String name, Map<String, Object> metadata) {
        List<String> modes = randomSubsetOf(Arrays.stream(Alternative.values()).map(Alternative::toString).collect(Collectors.toList()));
        return new InternalKSTestAggregation(
            name,
            metadata,
            modes.stream().collect(Collectors.toMap(Function.identity(), a -> randomDouble()))
        );
    }

    @Override
    protected void assertReduced(InternalKSTestAggregation reduced, List<InternalKSTestAggregation> inputs) {
        // no test since reduce operation is unsupported
    }

    @Override
    public void testReduceRandom() {
        expectThrows(UnsupportedOperationException.class, () -> createTestInstance("name", null).reduce(null, null));
    }

    @Override
    protected void assertFromXContent(InternalKSTestAggregation aggregation, ParsedAggregation parsedAggregation) {
        ParsedKSTest ks = (ParsedKSTest) parsedAggregation;
        assertThat(ks.getModes(), equalTo(aggregation.getModeValues()));
    }

    @Override
    protected InternalKSTestAggregation mutateInstance(InternalKSTestAggregation instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }
}
