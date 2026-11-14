/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.aggregations;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.DelayableWriteable;
import org.elasticsearch.common.io.stream.FilterStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalOrder.CompoundOrder;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.metrics.InternalAvg;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParser.Token;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class InternalOrderTests extends AbstractXContentSerializingTestCase<BucketOrder> {

    @Override
    protected BucketOrder createTestInstance() {
        if (randomBoolean()) {
            return getRandomOrder();
        } else {
            List<BucketOrder> orders = new ArrayList<>();
            for (int i = 0; i < randomInt(3); i++) {
                orders.add(getRandomOrder());
            }
            return BucketOrder.compound(orders);
        }
    }

    private BucketOrder getRandomOrder() {
        return switch (randomInt(2)) {
            case 0 -> BucketOrder.key(randomBoolean());
            case 1 -> BucketOrder.count(randomBoolean());
            default -> BucketOrder.aggregation(randomAlphaOfLength(10), randomBoolean());
        };
    }

    @Override
    protected Reader<BucketOrder> instanceReader() {
        return InternalOrder.Streams::readOrder;
    }

    @Override
    protected BucketOrder doParseInstance(XContentParser parser) throws IOException {
        Token token = parser.nextToken();
        if (token == Token.START_OBJECT) {
            return InternalOrder.Parser.parseOrderParam(parser);
        }
        if (token == Token.START_ARRAY) {
            List<BucketOrder> orders = new ArrayList<>();
            while (parser.nextToken() == Token.START_OBJECT) {
                orders.add(InternalOrder.Parser.parseOrderParam(parser));
            }
            return BucketOrder.compound(orders);
        }
        return null;
    }

    @Override
    protected void assertEqualInstances(BucketOrder expectedInstance, BucketOrder newInstance) {
        // identical behavior to AbstractSerializingTestCase, except assertNotSame is only called for
        // compound and aggregation order because _key and _count orders are static instances.
        assertEquals(expectedInstance, newInstance);
        assertEquals(expectedInstance.hashCode(), newInstance.hashCode());
        if (expectedInstance instanceof CompoundOrder || expectedInstance instanceof InternalOrder.Aggregation) {
            assertNotSame(newInstance, expectedInstance);
        }
    }

    public void testAggregationOrderEqualsAndHashCode() {
        String path = randomAlphaOfLength(10);
        boolean asc = randomBoolean();
        BucketOrder o1 = BucketOrder.aggregation(path, asc);
        BucketOrder o2 = BucketOrder.aggregation(path + "test", asc);
        BucketOrder o3 = BucketOrder.aggregation(path, asc == false);
        BucketOrder o4 = BucketOrder.aggregation(path, asc);
        assertNotEquals(o1, o2);
        assertNotEquals(o1.hashCode(), o2.hashCode());
        assertNotEquals(o1, o3);
        assertNotEquals(o1.hashCode(), o3.hashCode());
        assertEquals(o1, o4);
        assertEquals(o1.hashCode(), o4.hashCode());

        o1 = InternalOrder.compound(o1);
        o2 = InternalOrder.compound(o2);
        o3 = InternalOrder.compound(o3);
        assertNotEquals(o1, o2);
        assertNotEquals(o1.hashCode(), o2.hashCode());
        assertNotEquals(o1, o2);
        assertNotEquals(o1.hashCode(), o2.hashCode());
        assertNotEquals(o1, o3);
        assertNotEquals(o1.hashCode(), o3.hashCode());
        assertNotEquals(o1, o4);
        assertNotEquals(o1.hashCode(), o4.hashCode());
    }

    @Override
    protected BucketOrder mutateInstance(BucketOrder instance) {
        if (instance == InternalOrder.KEY_ASC) {
            return InternalOrder.COUNT_ASC;
        } else if (instance == InternalOrder.KEY_DESC) {
            return InternalOrder.KEY_ASC;
        } else if (instance == InternalOrder.COUNT_ASC) {
            return BucketOrder.aggregation(randomAlphaOfLengthBetween(1, 20), randomBoolean());
        } else if (instance == InternalOrder.COUNT_DESC) {
            return BucketOrder.compound(getRandomOrder());
        } else if (instance instanceof InternalOrder.Aggregation) {
            return InternalOrder.COUNT_DESC;
        } else {
            return InternalOrder.KEY_DESC;
        }
    }

    public void testInternalOrderDeduplicated() throws IOException {
        BucketOrder testInstance = createTestInstance();
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            instanceWriter().write(output, testInstance);
            if (testInstance instanceof CompoundOrder || testInstance instanceof InternalOrder.Aggregation) {
                assertNotSame(testInstance, instanceReader().read(output.bytes().streamInput()));
            }
            StreamInput dedupe = new DeduplicatorStreamInput(output.bytes().streamInput(), testInstance);
            assertSame(testInstance, instanceReader().read(dedupe));
        }
    }

    /**
     * Buckets whose ordering sub-aggregation has no value (an {@code avg} over zero documents) sort after
     * all buckets that have a value, in both ascending and descending order.
     */
    public void testAggregationComparatorSortsEmptyValuesLast() {
        for (boolean asc : new boolean[] { true, false }) {
            List<StringTerms.Bucket> buckets = new ArrayList<>(
                List.of(
                    avgBucket("low", 1, 1),
                    avgBucket("empty_1", 0, 0),
                    avgBucket("high", 30, 1),
                    avgBucket("empty_2", 0, 0),
                    avgBucket("mid", 20, 2)
                )
            );
            Collections.shuffle(buckets, random());
            Comparator<MultiBucketsAggregation.Bucket> comparator = BucketOrder.aggregation("avg", asc).comparator();
            buckets.sort(comparator);
            List<String> keys = buckets.stream().map(StringTerms.Bucket::getKeyAsString).toList();
            if (asc) {
                assertThat(keys.subList(0, 3), contains("low", "mid", "high"));
            } else {
                assertThat(keys.subList(0, 3), contains("high", "mid", "low"));
            }
            assertThat(keys.subList(3, 5), containsInAnyOrder("empty_1", "empty_2"));
            assertEquals(0, comparator.compare(buckets.get(3), buckets.get(4)));
        }
    }

    private static StringTerms.Bucket avgBucket(String key, double sum, long count) {
        InternalAggregations aggs = InternalAggregations.from(new InternalAvg("avg", sum, count, DocValueFormat.RAW, null));
        return new StringTerms.Bucket(new BytesRef(key), randomLongBetween(1, 100), aggs, false, 0, DocValueFormat.RAW);
    }

    private static class DeduplicatorStreamInput extends FilterStreamInput implements DelayableWriteable.Deduplicator {

        private final BucketOrder order;

        protected DeduplicatorStreamInput(StreamInput delegate, BucketOrder order) {
            super(delegate);
            this.order = order;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> T deduplicate(T object) {
            return (T) order;
        }
    }

}
