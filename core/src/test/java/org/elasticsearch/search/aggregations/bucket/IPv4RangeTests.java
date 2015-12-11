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

import org.elasticsearch.common.network.Cidrs;
import org.elasticsearch.index.mapper.ip.IpFieldMapper;
import org.elasticsearch.search.aggregations.BaseAggregationTestCase;
import org.elasticsearch.search.aggregations.bucket.range.ipv4.IPv4RangeAggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.range.ipv4.IPv4RangeAggregatorFactory.Range;

import java.util.ArrayList;
import java.util.List;

public class IPv4RangeTests extends BaseAggregationTestCase<IPv4RangeAggregatorFactory> {

    @Override
    protected IPv4RangeAggregatorFactory createTestAggregatorFactory() {
        int numRanges = randomIntBetween(1, 10);
        List<Range> ranges = new ArrayList<>(numRanges);
        for (int i = 0; i < numRanges; i++) {
            String key = null;
            if (randomBoolean()) {
                key = randomAsciiOfLengthBetween(1, 20);
            }
            if (randomBoolean()) {
                double from = randomBoolean() ? Double.NEGATIVE_INFINITY : randomIntBetween(Integer.MIN_VALUE, Integer.MAX_VALUE - 1000);
                double to = randomBoolean() ? Double.POSITIVE_INFINITY
                        : (Double.isInfinite(from) ? randomIntBetween(Integer.MIN_VALUE, Integer.MAX_VALUE)
                                : randomIntBetween((int) from, Integer.MAX_VALUE));
                if (randomBoolean()) {
                    ranges.add(new Range(key, from, to));
                } else {
                    String fromAsStr = Double.isInfinite(from) ? null : IpFieldMapper.longToIp((long) from);
                    String toAsStr = Double.isInfinite(to) ? null : IpFieldMapper.longToIp((long) to);
                    ranges.add(new Range(key, fromAsStr, toAsStr));
                }
            } else {
                int mask = randomInt(32);
                long ipAsLong = randomIntBetween(0, Integer.MAX_VALUE);

                long blockSize = 1L << (32 - mask);
                ipAsLong = ipAsLong - (ipAsLong & (blockSize - 1));
                String cidr = Cidrs.createCIDR(ipAsLong, mask);
                ranges.add(new Range(key, cidr));
            }
        }
        IPv4RangeAggregatorFactory factory = new IPv4RangeAggregatorFactory("foo", ranges);
        factory.field(INT_FIELD_NAME);
        if (randomBoolean()) {
            factory.keyed(randomBoolean());
        }
        if (randomBoolean()) {
            factory.missing(randomIntBetween(0, 10));
        }
        return factory;
    }

}
