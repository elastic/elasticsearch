/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.bucket;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.search.aggregations.BaseAggregationTestCase;
import org.elasticsearch.search.aggregations.bucket.range.ip.IpRangeAggregationBuilder;

public class IpRangeTests extends BaseAggregationTestCase<IpRangeAggregationBuilder> {

    private static String randomIp(boolean v4) {
        try {
            if (v4) {
                byte[] ipv4 = new byte[4];
                random().nextBytes(ipv4);
                return NetworkAddress.format(InetAddress.getByAddress(ipv4));
            } else {
                byte[] ipv6 = new byte[16];
                random().nextBytes(ipv6);
                return NetworkAddress.format(InetAddress.getByAddress(ipv6));
            }
        } catch (UnknownHostException e) {
            throw new AssertionError();
        }
    }

    @Override
    protected IpRangeAggregationBuilder createTestAggregatorBuilder() {
        int numRanges = randomIntBetween(1, 10);
        IpRangeAggregationBuilder factory = new IpRangeAggregationBuilder("foo");
        for (int i = 0; i < numRanges; i++) {
            String key = null;
            if (randomBoolean()) {
                key = randomAsciiOfLengthBetween(1, 20);
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
                factory.addMaskRange(key, randomIp(v4) + "/" + prefixLength);
                break;
            case 1:
                factory.addUnboundedFrom(key, randomIp(randomBoolean()));
                break;
            case 2:
                factory.addUnboundedTo(key, randomIp(randomBoolean()));
                break;
            case 3:
                factory.addRange(key, randomIp(randomBoolean()), randomIp(randomBoolean()));
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
            factory.missing(randomIp(randomBoolean()));
        }
        return factory;
    }

}
