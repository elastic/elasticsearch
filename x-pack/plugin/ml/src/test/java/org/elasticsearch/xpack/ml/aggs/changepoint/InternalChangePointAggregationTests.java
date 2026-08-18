/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.changepoint;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class InternalChangePointAggregationTests extends AbstractWireSerializingTestCase<InternalChangePointAggregation> {

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>();
        namedWriteables.addAll(new ChangePointNamedContentProvider().getNamedWriteables());
        namedWriteables.addAll(new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedWriteables());
        return new NamedWriteableRegistry(namedWriteables);
    }

    @Override
    protected Writeable.Reader<InternalChangePointAggregation> instanceReader() {
        return InternalChangePointAggregation::new;
    }

    @Override
    protected InternalChangePointAggregation createTestInstance() {
        int n = randomIntBetween(0, 3);
        List<ChangeType> changeTypes = new ArrayList<>(n);
        List<ChangePointBucket> buckets = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            ChangeType c = randomFrom(
                new ChangeType.Stationary(),
                new ChangeType.NonStationary(randomDouble(), randomDouble(), randomAlphaOfLength(10)),
                new ChangeType.Dip(randomDouble(), randomInt(1000), randomAlphaOfLength(10)),
                new ChangeType.Spike(randomDouble(), randomInt(1000), randomAlphaOfLength(10)),
                new ChangeType.TrendChange(randomDouble(), randomDouble(), randomInt(1000), randomAlphaOfLength(10)),
                new ChangeType.DistributionChange(randomDouble(), randomInt(1000), randomAlphaOfLength(10))
            );
            changeTypes.add(c);
            buckets.add(
                c.isChange() && randomBoolean()
                    ? new ChangePointBucket(randomAlphaOfLength(10), randomNonNegativeLong(), InternalAggregations.EMPTY)
                    : null
            );
        }
        return new InternalChangePointAggregation(randomAlphaOfLength(10), Collections.singletonMap("foo", "bar"), buckets, changeTypes);
    }

    @Override
    protected InternalChangePointAggregation mutateInstance(InternalChangePointAggregation instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }
}
