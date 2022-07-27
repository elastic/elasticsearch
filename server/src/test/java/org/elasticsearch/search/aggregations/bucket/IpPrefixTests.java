/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.search.aggregations.BaseAggregationTestCase;
import org.elasticsearch.search.aggregations.bucket.prefix.IpPrefixAggregationBuilder;

import static org.hamcrest.Matchers.startsWith;

public class IpPrefixTests extends BaseAggregationTestCase<IpPrefixAggregationBuilder> {
    @Override
    protected IpPrefixAggregationBuilder createTestAggregatorBuilder() {
        final String name = randomAlphaOfLengthBetween(3, 10);
        final IpPrefixAggregationBuilder factory = new IpPrefixAggregationBuilder(name);
        boolean isIpv6 = randomBoolean();
        int prefixLength = isIpv6 ? randomIntBetween(1, 128) : randomIntBetween(1, 32);
        factory.field(IP_FIELD_NAME);

        factory.appendPrefixLength(randomBoolean());
        factory.isIpv6(isIpv6);
        factory.prefixLength(prefixLength);
        factory.keyed(randomBoolean());
        factory.minDocCount(randomIntBetween(1, 3));

        return factory;
    }

    public void testNegativePrefixLength() {
        final IpPrefixAggregationBuilder factory = new IpPrefixAggregationBuilder(randomAlphaOfLengthBetween(3, 10));
        boolean isIpv6 = randomBoolean();
        final String rangeAsString = isIpv6 ? "[0, 128]" : "[0, 32]";
        factory.isIpv6(isIpv6);
        int randomPrefixLength = randomIntBetween(-1000, -1);

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> factory.prefixLength(randomPrefixLength));
        assertThat(
            ex.getMessage(),
            startsWith("[prefix_length] must be in range " + rangeAsString + " while value is [" + randomPrefixLength + "]")
        );
    }
}
