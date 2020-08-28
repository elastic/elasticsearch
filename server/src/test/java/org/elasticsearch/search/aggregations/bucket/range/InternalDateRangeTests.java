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

package org.elasticsearch.search.aggregations.bucket.range;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class InternalDateRangeTests extends InternalRangeTestCase<InternalDateRange> {

    private DocValueFormat format;
    private List<Tuple<Double, Double>> dateRanges;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        format = randomNumericDocValueFormat();

        Function<DateTime, DateTime> interval = randomFrom(dateTime -> dateTime.plusSeconds(1), dateTime -> dateTime.plusMinutes(1),
                dateTime -> dateTime.plusHours(1), dateTime -> dateTime.plusDays(1), dateTime -> dateTime.plusMonths(1), dateTime ->
                        dateTime.plusYears(1));

        final int numRanges = randomNumberOfBuckets();
        final List<Tuple<Double, Double>> listOfRanges = new ArrayList<>(numRanges);

        DateTime date = new DateTime(DateTimeZone.UTC);
        double start = date.getMillis();
        double end = 0;
        for (int i = 0; i < numRanges; i++) {
            double from = date.getMillis();
            date = interval.apply(date);
            double to = date.getMillis();
            if (to > end) {
                end = to;
            }

            if (randomBoolean()) {
                listOfRanges.add(Tuple.tuple(from, to));
            } else {
                // Add some overlapping range
                listOfRanges.add(Tuple.tuple(start, randomDoubleBetween(start, end, false)));
            }
        }
        Collections.shuffle(listOfRanges, random());
        dateRanges = Collections.unmodifiableList(listOfRanges);
    }

    @Override
    protected InternalDateRange createTestInstance(String name,
                                                   Map<String, Object> metadata,
                                                   InternalAggregations aggregations,
                                                   boolean keyed) {
        final List<InternalDateRange.Bucket> buckets = new ArrayList<>();
        for (int i = 0; i < dateRanges.size(); ++i) {
            Tuple<Double, Double> range = dateRanges.get(i);
            int docCount = randomIntBetween(0, 1000);
            double from = range.v1();
            double to = range.v2();
            buckets.add(new InternalDateRange.Bucket("range_" + i, from, to, docCount, aggregations, keyed, format));
        }
        return new InternalDateRange(name, buckets, format, keyed, metadata);
    }

    @Override
    protected Class<? extends ParsedMultiBucketAggregation> implementationClass() {
        return ParsedDateRange.class;
    }

    @Override
    protected Class<? extends InternalMultiBucketAggregation.InternalBucket> internalRangeBucketClass() {
        return InternalDateRange.Bucket.class;
    }

    @Override
    protected Class<? extends ParsedMultiBucketAggregation.ParsedBucket> parsedRangeBucketClass() {
        return ParsedDateRange.ParsedBucket.class;
    }

    @Override
    protected InternalDateRange mutateInstance(InternalDateRange instance) {
        String name = instance.getName();
        DocValueFormat format = instance.format;
        boolean keyed = instance.keyed;
        List<InternalDateRange.Bucket> buckets = instance.getBuckets();
        Map<String, Object> metadata = instance.getMetadata();
        switch (between(0, 3)) {
        case 0:
            name += randomAlphaOfLength(5);
            break;
        case 1:
            keyed = keyed == false;
            break;
        case 2:
            buckets = new ArrayList<>(buckets);
            double from = randomDouble();
            buckets.add(new InternalDateRange.Bucket("range_a", from, from + randomDouble(), randomNonNegativeLong(),
                    InternalAggregations.EMPTY, false, format));
            break;
        case 3:
            if (metadata == null) {
                metadata = new HashMap<>(1);
            } else {
                metadata = new HashMap<>(instance.getMetadata());
            }
            metadata.put(randomAlphaOfLength(15), randomInt());
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }
        return new InternalDateRange(name, buckets, format, keyed, metadata);
    }
}
