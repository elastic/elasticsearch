/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.search.aggregations.BaseAggregationTestCase;
import org.elasticsearch.search.aggregations.bucket.range.IpRangeAggregationBuilder;

public class IpRangeTests extends BaseAggregationTestCase<IpRangeAggregationBuilder> {

    @Override
    protected IpRangeAggregationBuilder createTestAggregatorBuilder() {
        int numRanges = randomIntBetween(1, 10);
        IpRangeAggregationBuilder factory = new IpRangeAggregationBuilder(randomAlphaOfLengthBetween(3, 10));
        for (int i = 0; i < numRanges; i++) {
            String key = null;
            if (randomBoolean()) {
                key = randomAlphaOfLengthBetween(1, 20);
            }
            switch (randomInt(3)) {
            case 0:
                boolean v4 = randomBoolean();
                int prefixLength;
                if (v4) {
                    prefixLength = randomInt(32);
                } else {
                    prefixLength = randomInt(128);
                }
                factory.addMaskRange(key, NetworkAddress.format(randomIp(v4)) + "/" + prefixLength);
                break;
            case 1:
                factory.addUnboundedFrom(key, NetworkAddress.format(randomIp(randomBoolean())));
                break;
            case 2:
                factory.addUnboundedTo(key, NetworkAddress.format(randomIp(randomBoolean())));
                break;
            case 3:
                v4 = randomBoolean();
                factory.addRange(key, NetworkAddress.format(randomIp(v4)), NetworkAddress.format(randomIp(v4)));
                break;
            default:
                fail();
            }
        }
        factory.field(IP_FIELD_NAME);
        if (randomBoolean()) {
            factory.keyed(randomBoolean());
        }
        if (randomBoolean()) {
            factory.missing(NetworkAddress.format(randomIp(randomBoolean())));
        }
        return factory;
    }

    public void testMask() {
        IpRangeAggregationBuilder b1 = new IpRangeAggregationBuilder("foo");
        IpRangeAggregationBuilder b2 = new IpRangeAggregationBuilder("foo");
        b1.addMaskRange("bar", "192.168.10.12/16");
        b2.addRange("bar", "192.168.0.0", "192.169.0.0");
        assertEquals(b1, b2);

        b1 = new IpRangeAggregationBuilder("foo");
        b2 = new IpRangeAggregationBuilder("foo");
        b1.addMaskRange("bar", "192.168.0.0/31");
        b2.addRange("bar", "192.168.0.0", "192.168.0.2");
        assertEquals(b1, b2);

        b1 = new IpRangeAggregationBuilder("foo");
        b2 = new IpRangeAggregationBuilder("foo");
        b1.addMaskRange("bar", "0.0.0.0/0");
        b2.addRange("bar", "0.0.0.0", "::1:0:0:0");
        assertEquals(b1, b2);

        b1 = new IpRangeAggregationBuilder("foo");
        b2 = new IpRangeAggregationBuilder("foo");
        b1.addMaskRange("bar", "fe80::821f:2ff:fe4a:c5bd/64");
        b2.addRange("bar", "fe80::", "fe80:0:0:1::");
        assertEquals(b1, b2);

        b1 = new IpRangeAggregationBuilder("foo");
        b2 = new IpRangeAggregationBuilder("foo");
        b1.addMaskRange("bar", "::/16");
        b2.addRange("bar", null, "1::");
        assertEquals(b1, b2);

        b1 = new IpRangeAggregationBuilder("foo");
        b2 = new IpRangeAggregationBuilder("foo");
        b1.addMaskRange("bar", "::/0");
        b2.addRange("bar", null, null);
        assertEquals(b1, b2);
    }
}
