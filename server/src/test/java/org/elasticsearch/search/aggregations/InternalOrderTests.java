/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.aggregations.InternalOrder.CompoundOrder;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParser.Token;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class InternalOrderTests extends AbstractSerializingTestCase<BucketOrder> {

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
    protected BucketOrder mutateInstance(BucketOrder instance) throws IOException {
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

}
